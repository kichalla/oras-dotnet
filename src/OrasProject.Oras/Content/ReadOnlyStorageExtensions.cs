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

using OrasProject.Oras.Oci;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace OrasProject.Oras.Content;

/// <summary>
/// ReadOnlyStorageExtensions provides extension helpers for read-only storage.
/// </summary>
public static class ReadOnlyStorageExtensions
{
    /// <summary>
    /// CopyGraphAsync concurrently copy node desc from src to dst 
    /// </summary>
    /// <param name="src"></param>
    /// <param name="dst"></param>
    /// <param name="node"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="InvalidOperationException">The dependency graph contains a cycle.</exception>
    public static async Task CopyGraphAsync(
        this IReadOnlyStorage src,
        IStorage dst,
        Descriptor node,
        CancellationToken cancellationToken = default)
    {
        var copyGraphOptions = new CopyGraphOptions();
        var proxy = new Proxy()
        {
            Cache = new LimitedStorage(new MemoryStorage(), copyGraphOptions.MaxMetadataBytes),
            Source = src
        };
        await src.CopyGraphAsync(dst, node, proxy, copyGraphOptions, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// CopyGraphAsync concurrently copy node from src to dst by using customized copyGraphOptions
    /// </summary>
    /// <param name="src"></param>
    /// <param name="dst"></param>
    /// <param name="node"></param>
    /// <param name="copyGraphOptions"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="InvalidOperationException">The dependency graph contains a cycle.</exception>
    public static async Task CopyGraphAsync(
        this IReadOnlyStorage src,
        IStorage dst,
        Descriptor node,
        CopyGraphOptions copyGraphOptions,
        CancellationToken cancellationToken = default)
    {
        var proxy = new Proxy()
        {
            Cache = new LimitedStorage(new MemoryStorage(), copyGraphOptions.MaxMetadataBytes),
            Source = src
        };
        await src.CopyGraphAsync(dst, node, proxy, copyGraphOptions, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// CopyGraphAsync concurrently copy node from src to dst by using customized copyGraphOptions and cached proxy
    /// </summary>
    /// <param name="src"></param>
    /// <param name="dst"></param>
    /// <param name="node"></param>
    /// <param name="proxy"></param>
    /// <param name="copyGraphOptions"></param>
    /// <param name="cancellationToken"></param>
    internal static async Task CopyGraphAsync(
        this IReadOnlyStorage src,
        IStorage dst,
        Descriptor node,
        Proxy proxy,
        CopyGraphOptions copyGraphOptions,
        CancellationToken cancellationToken = default)
    {
        var operation = new CopyGraphOperation(dst, proxy, copyGraphOptions);
        await operation.RunAsync(new[] { node }, cancellationToken).ConfigureAwait(false);
    }
}
