"""
Generic (enhanced) read-write locks, which arguably should belong in a more general-purpose package.

These add functionality on top of `ConcurrentUtils`; specifically, they allow querying the status of the lock.

We do not re-export the types and functions defined here from the top-level `Daf` namespace. That is, even if
`using Daf`, you will **not** have these generic names polluting your namespace. If you do want to reuse them in your
code, explicitly write `using Daf.GenericLocks`.

!!! note

    This code relies on tasks staying on the same thread, so **always** specify `@threads :static` when using them,
    otherwise bad things will happen when Mercury is in retrograde.
"""
module GenericLocks

export QueryReadWriteLock
export has_read_lock
export has_write_lock
export read_lock
export read_unlock
export with_read_lock
export with_write_lock
export write_lock
export write_unlock

using Base.Threads
using ConcurrentUtils

mutable struct WriterThread
    thread_id::Integer
    depth::Int
end

"""
    struct QueryReadWriteLock <: AbstractLock ... end

A read-write lock with queries.
"""
struct QueryReadWriteLock
    lock::ReadWriteLock
    writer_thread::Vector{WriterThread}
    read_depth_of_threads::Vector{Int}
end

function QueryReadWriteLock()
    return QueryReadWriteLock(ReadWriteLock(), [WriterThread(0, 0)], fill(0, nthreads()))
end

"""
    write_lock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing

Obtain a write lock. Each call must be matched by [`write_unlock`](@ref). It is possible to nest
`write_lock`/`write_unlock` call pairs.

When a thread has a write lock, no other thread can have any lock.

The log messages includes `what` is being locked.
"""
function write_lock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    read_key = Symbol((lock_id, false))
    @assert !haskey(private_storage, read_key)

    write_depth = get(private_storage, write_key, nothing)
    if write_depth !== nothing
        write_depth[1] += 1
    else
        private_storage[write_key] = [1]
        @debug "WLOCK $(lock_id) $(join(what, " ")) {{{"
        lock(query_read_write_lock.lock)
        @debug "WLOCKED $(lock_id) $(join(what, " "))"
    end

    return nothing
end

"""
    write_unlock(query_read_write_lock::QueryReadWriteLock, what::AbstractString)::Nothing

Release a write lock. Each call must matched a call to [`write_lock`](@ref). It is possible to nest
`write_lock`/`write_unlock` call pairs.

The log messages includes `what` is being unlocked.
"""
function write_unlock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    read_key = Symbol((lock_id, false))
    @assert !haskey(private_storage, read_key)
    @assert haskey(private_storage, write_key)

    write_depth = private_storage[write_key]
    if write_depth[1] > 1
        write_depth[1] -= 1
    else
        @assert write_depth[1] == 1
        delete!(private_storage, write_key)
        unlock(query_read_write_lock.lock)
        @debug "WUNLOCKED $(lock_id) $(join(what, " ")) }}}"
    end
    return nothing
end

"""
    has_write_lock(query_read_write_lock::QueryReadWriteLock)::Bool

Return whether the current thread has the write lock.
"""
function has_write_lock(query_read_write_lock::QueryReadWriteLock)::Bool
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    return haskey(private_storage, write_key)
end

"""
    read_lock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing

Obtain a read lock. Each call must be matched by [`read_unlock`](@ref). It is possible to nest `read_lock`/`read_unlock`
call pairs, even inside `write_lock`/`write_unlock` pair(s); however, you can't nest `write_lock`/`write_unlock` inside
a `read_lock`/`read_unlock` pair.

When a thread has a read lock, no other thread can have a write lock, but other threads may also have a read lock.

The log messages includes `what` is being locked.
"""
function read_lock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    read_key = Symbol((lock_id, false))

    read_depth = get(private_storage, read_key, nothing)
    if read_depth !== nothing
        read_depth[1] += 1
    else
        private_storage[read_key] = [1]
        if !haskey(private_storage, write_key)
            @debug "RLOCK $(lock_id) $(join(what, " ")) {{{"
            lock_read(query_read_write_lock.lock)
            @debug "RLOCKED $(lock_id) $(join(what, " "))"
        end
    end

    return nothing
end

"""
    read_unlock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing

Release a read lock. Each call must matched a call to [`read_lock`](@ref). It is possible to nest
`read_lock`/`read_unlock` call pairs.

The log messages includes `what` is being unlocked.
"""
function read_unlock(query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    read_key = Symbol((lock_id, false))
    @assert haskey(private_storage, read_key)

    read_depth = private_storage[read_key]
    if read_depth[1] > 1
        read_depth[1] -= 1
    else
        @assert read_depth[1] == 1
        delete!(private_storage, read_key)
        if !haskey(private_storage, write_key)
            unlock_read(query_read_write_lock.lock)
            @debug "RUNLOCKED $(lock_id) $(join(what, " ")) }}}"
        end
    end

    return nothing
end

"""
    has_read_lock(query_read_write_lock::QueryReadWriteLock; read_only::Bool = false)::Bool

Return whether the current thread has a read lock or the write lock. If `read_only` is set, then this will only return
whether the current thread as a read lock.
"""
function has_read_lock(query_read_write_lock::QueryReadWriteLock; read_only::Bool = false)::Bool
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    return haskey(private_storage, Symbol((lock_id, false))) ||
           (!read_only && haskey(private_storage, Symbol((lock_id, true))))
end

"""
    with_write_lock(action::Function, query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Any

Perform an `action` while holding a [`write_lock`](@ref) for the `query_read_write_lock`, return
its result and [`write_unlock`](@ref).
"""
function with_write_lock(action::Function, query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Any
    write_lock(query_read_write_lock, what...)
    try
        return action()
    finally
        write_unlock(query_read_write_lock, what...)
    end
end

"""
    with_read_lock(action::Function, query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Any

Perform an `action` while holding a [`read_lock`](@ref) for the `query_read_write_lock`, return
its result and [`read_unlock`](@ref).
"""
function with_read_lock(action::Function, query_read_write_lock::QueryReadWriteLock, what::AbstractString...)::Any
    read_lock(query_read_write_lock, what...)
    try
        return action()
    finally
        read_unlock(query_read_write_lock, what...)
    end
end

end
