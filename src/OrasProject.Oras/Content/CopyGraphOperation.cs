// Copyright The ORAS Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using OrasProject.Oras.Oci;

namespace OrasProject.Oras.Content;

/// <summary>
/// Copies a graph with a streaming dependency-count scheduler: discover each node's
/// successors once, then copy it when all of its dependency references have finished.
/// </summary>
/// <remarks>
/// RunAsync is the sole writer of graph state. Workers perform I/O or callbacks and
/// report completion; they never wait for another node or update dependency counts.
/// Waiting for dependencies therefore occupies no worker slot.
///
/// The normal node path is discovery -> waiting for dependencies -> copy -> success.
/// Existing destination content skips the remaining path. Failed dependencies prevent
/// a parent's copy, while independent branches can continue.
/// </remarks>
internal sealed class CopyGraphOperation
{
    private readonly IStorage _destination;
    private readonly Proxy _proxy;
    private readonly CopyGraphOptions _options;
    private readonly Dictionary<BasicDescriptor, Node> _nodes;
    private readonly Queue<Work> _ready = new();
    private readonly Queue<Work> _skipped = new();
    // Terminal nodes awaiting reference notifications, not worker completion events.
    private readonly Queue<Node> _completed = new();
    private readonly Dictionary<Work, Task> _active = new();
    private readonly Channel<Completion> _completions = Channel.CreateUnbounded<Completion>(
        new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
    private ExceptionDispatchInfo? _failure;
    // Discovery/copy slots only; _active also includes duplicate-reference callbacks.
    private int _running;
    private int _remainingRoots;

    internal CopyGraphOperation(
        IStorage destination,
        Proxy proxy,
        CopyGraphOptions options,
        IEqualityComparer<BasicDescriptor>? comparer = null)
    {
        _destination = destination;
        _proxy = proxy;
        _options = options;
        _nodes = new Dictionary<BasicDescriptor, Node>(comparer);
    }

    internal async Task RunAsync(IEnumerable<Descriptor> roots, CancellationToken cancellationToken = default)
    {
        var rootList = roots.ToList();
        ValidateDescriptors(rootList);
        _remainingRoots = rootList.Count;
        foreach (var root in rootList)
        {
            AddReference(root, null);
        }

        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                // Finishing a reference may release its parent. Apply those consequences
                // before dispatching more work or deciding whether the graph is stuck.
                PropagateCompletions();
                // Duplicate-reference callbacks have never consumed copy concurrency slots.
                while (_skipped.TryDequeue(out var skipped))
                {
                    Start(skipped, cancellationToken);
                }
                while (_running < _options.Concurrency && _ready.TryDequeue(out var work))
                {
                    Start(work, cancellationToken);
                }

                if (_active.Count == 0)
                {
                    // Both runnable queues have been dispatched, and no worker can report
                    // another event. Without a failure, unfinished roots now imply a cycle.
                    // An empty queue while discovery, I/O, or callbacks are active does not.
                    _failure?.Throw();
                    if (_remainingRoots != 0)
                    {
                        throw new InvalidOperationException("Dependency cycle detected while copying graph.");
                    }
                    return;
                }

                var completion = await _completions.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                // The worker has reported after cleanup, but its Task may still be returning.
                // Join before removing it so the cancellation drain cannot miss a worker.
                await _active[completion.Work].ConfigureAwait(false);
                _active.Remove(completion.Work);
                if (completion.Work.Kind != WorkKind.SkipReference)
                {
                    _running--;
                }
                Complete(completion);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Stop admitting work, but collect faults from already-started workers after
            // their cleanup finishes. Do not release more parents during cancellation.
            await Task.WhenAll(_active.Values).ConfigureAwait(false);
            while (_completions.Reader.TryRead(out var completion))
            {
                if (completion.Error is { } error)
                {
                    RecordFailure(error);
                }
            }
            // Like WhenAll, an actual fault takes precedence over canceled peer operations.
            _failure?.Throw();
            throw;
        }
        finally
        {
            // In particular, cancellation must not return before callback/stream cleanup finishes.
            await Task.WhenAll(_active.Values).ConfigureAwait(false);
        }
    }

    private void AddReference(Descriptor descriptor, Node? parent)
    {
        // Deduplicate work by content identity, but retain each occurrence's full descriptor:
        // duplicate callbacks must receive that occurrence's annotations, not the owner's.
        var key = descriptor.BasicDescriptor;
        var duplicate = true;
        if (!_nodes.TryGetValue(key, out var node))
        {
            duplicate = false;
            node = new Node(descriptor);
            _nodes.Add(key, node);
            _ready.Enqueue(new Work(node, WorkKind.Discover));
        }
        var reference = new Reference(descriptor, parent, duplicate);
        if (node.State is NodeState.Succeeded or NodeState.Failed)
        {
            CompleteReference(node, reference);
        }
        else
        {
            node.References.Add(reference);
        }
    }

    private void CompleteReference(Node node, Reference reference)
    {
        // Shared node success is not enough for a duplicate reference: its callback must
        // also finish. Callback failure blocks this reference's parent, not the shared node.
        if (node.State == NodeState.Succeeded && reference.Duplicate && _options.OnCopySkippedAsync != null)
        {
            _skipped.Enqueue(new Work(node, WorkKind.SkipReference, reference));
        }
        else
        {
            FinishReference(reference, node.State == NodeState.Succeeded);
        }
    }

    private void FinishReference(Reference reference, bool succeeded)
    {
        if (reference.Parent is not { } parent)
        {
            _remainingRoots--;
            return;
        }
        parent.DependencyFailed |= !succeeded;
        if (--parent.PendingDependencies == 0)
        {
            if (parent.DependencyFailed)
            {
                FinishNode(parent, false);
            }
            else
            {
                ReadyToCopy(parent);
            }
        }
    }

    private void ReadyToCopy(Node node)
    {
        node.State = NodeState.ReadyToCopy;
        _ready.Enqueue(new Work(node, WorkKind.Copy));
    }

    private void FinishNode(Node node, bool succeeded)
    {
        node.State = succeeded ? NodeState.Succeeded : NodeState.Failed;
        _completed.Enqueue(node);
    }

    private void PropagateCompletions()
    {
        // Consume each incoming reference once, rather than rescanning the graph.
        // Iteration also avoids recursive unwinding through long chains of failed parents.
        while (_completed.TryDequeue(out var node))
        {
            foreach (var reference in node.References)
            {
                CompleteReference(node, reference);
            }
            node.References.Clear();
        }
    }

    private void Start(Work work, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (work.Kind != WorkKind.SkipReference)
        {
            _running++;
            work.Node.State = work.Kind == WorkKind.Discover ? NodeState.Discovering : NodeState.Copying;
        }
        // Do not cancel task scheduling: every admitted worker must report exactly one completion.
        _active.Add(work, Task.Run(() => ExecuteAsync(work, cancellationToken)));
    }

    private async Task ExecuteAsync(Work work, CancellationToken cancellationToken)
    {
        List<Descriptor>? successors = null;
        Exception? error = null;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var descriptor = work.Node.Descriptor;
            if (work.Kind == WorkKind.SkipReference)
            {
                await _options.OnCopySkippedAsync!(work.Reference!.Descriptor, cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (work.Kind == WorkKind.Discover)
            {
                if (await _destination.ExistsAsync(descriptor, cancellationToken).ConfigureAwait(false))
                {
                    if (_options.OnCopySkippedAsync != null)
                    {
                        await _options.OnCopySkippedAsync(descriptor, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    successors = (await _options.FindSuccessorsAsync(_proxy, descriptor, cancellationToken)
                        .ConfigureAwait(false)).ToList();
                    ValidateDescriptors(successors);
                }
            }
            else
            {
                await CopyAsync(descriptor, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            // Failure is an explicit completion event; it never completes a dependency successfully.
            error = exception;
        }
        // Reporting must not be canceled: every admitted worker owes the coordinator one
        // result, including when its I/O or callback was canceled.
        await _completions.Writer.WriteAsync(new Completion(work, successors, error)).ConfigureAwait(false);
    }

    private async Task CopyAsync(Descriptor descriptor, CancellationToken cancellationToken)
    {
        if (_options.PreCopyAsync != null &&
            await _options.PreCopyAsync(descriptor, cancellationToken)
                .ConfigureAwait(false) == CopyNodeDecision.SkipNode)
        {
            return;
        }
        using var content = await _proxy.FetchAsync(descriptor, cancellationToken).ConfigureAwait(false);
        await _destination.PushAsync(descriptor, content, cancellationToken).ConfigureAwait(false);
        if (_options.PostCopyAsync != null)
        {
            await _options.PostCopyAsync(descriptor, cancellationToken).ConfigureAwait(false);
        }
    }

    private void Complete(Completion completion)
    {
        var work = completion.Work;
        if (completion.Error is { } error)
        {
            RecordFailure(error);
            if (work.Kind == WorkKind.SkipReference)
            {
                FinishReference(work.Reference!, false);
            }
            else
            {
                FinishNode(work.Node, false);
            }
        }
        else if (work.Kind == WorkKind.SkipReference)
        {
            FinishReference(work.Reference!, true);
        }
        else if (work.Kind == WorkKind.Discover && completion.Successors is { } successors)
        {
            var node = work.Node;
            node.State = NodeState.WaitingForDependencies;
            // Set the full count before linking children: an already-completed child can
            // satisfy its reference synchronously inside AddReference.
            node.PendingDependencies = successors.Count;
            if (successors.Count == 0)
            {
                ReadyToCopy(node);
            }
            foreach (var successor in successors)
            {
                AddReference(successor, node);
            }
        }
        else
        {
            FinishNode(work.Node, true);
        }
    }

    private void RecordFailure(Exception error)
    {
        if (_failure == null ||
            (_failure.SourceException is OperationCanceledException && error is not OperationCanceledException))
        {
            _failure = ExceptionDispatchInfo.Capture(error);
        }
    }

    private static void ValidateDescriptors(IEnumerable<Descriptor> descriptors)
    {
        foreach (var node in descriptors)
        {
            if (Descriptor.IsNullOrInvalid(node))
            {
                throw new ArgumentNullException(nameof(node));
            }
        }
    }

    private enum NodeState
    {
        PendingDiscovery,
        Discovering,
        WaitingForDependencies,
        ReadyToCopy,
        Copying,
        Succeeded,
        Failed
    }

    private enum WorkKind { Discover, Copy, SkipReference }

    // One shared copy per content identity. References are incoming occurrences waiting
    // for this node; PendingDependencies counts outgoing occurrences this node still needs.
    // Counts include duplicates because each duplicate callback can finish or fail separately.
    private sealed class Node(Descriptor descriptor)
    {
        internal Descriptor Descriptor { get; } = descriptor;
        internal NodeState State { get; set; }
        internal List<Reference> References { get; } = new();
        internal int PendingDependencies { get; set; }
        internal bool DependencyFailed { get; set; }
    }

    // One occurrence in a parent's successor list, or a root when Parent is null.
    // The first occurrence owns the shared work; later ones run skip callbacks on success.
    private sealed record Reference(Descriptor Descriptor, Node? Parent, bool Duplicate);

    private sealed class Work(Node node, WorkKind kind, Reference? reference = null)
    {
        internal Node Node { get; } = node;
        internal WorkKind Kind { get; } = kind;
        internal Reference? Reference { get; } = reference;
    }

    // Successful discovery returns null for existing content, or a complete (possibly empty)
    // successor list for missing content. Other work kinds carry no successor list.
    private sealed record Completion(Work Work, List<Descriptor>? Successors, Exception? Error);
}
