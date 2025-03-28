"""
Generic (reentrant) read-write locks, which arguably should belong in a more general-purpose package.

These add functionality on top of `ConcurrentUtils`; specifically, they allow querying the status of the lock, and
querying the status of the lock. We also provide explicit `write_*` and `read_*` functions instead of relying on `lock`
to be `write_lock`. That is, [`ReentrantReadWriteLock`](@ref) is **not** an `AbstractLock`.

!!! note

    We do not re-export the types and functions defined here from the top-level `DataAxesFormats` namespace. That is,
    even if `using DataAxesFormats`, you will **not** have these generic names polluting your namespace. If you do want
    to reuse them in your code, explicitly write `using DataAxesFormats.GenericLocks`.
"""
module GenericLocks

export ReentrantReadWriteLock
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

"""
    struct ReentrantReadWriteLock <: AbstractLock ... end

A read-write lock that supports nested calls. You can nest read locks, write locks, and read locks inside write locks.
However, you can't nest write locks in read locks.
"""
struct ReentrantReadWriteLock
    lock::ReadWriteLock
end

function ReentrantReadWriteLock()
    return ReentrantReadWriteLock(ReadWriteLock())
end

"""
    write_lock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Nothing

Obtain a write lock. Each call must be matched by [`write_unlock`](@ref). It is possible to nest
`write_lock`/`write_unlock` call pairs.

When a task has a write lock, no other task can have any lock.

The log messages includes `what` is being locked.
"""
function write_lock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))

    write_depth = get(private_storage, write_key, nothing)
    if write_depth !== nothing
        write_depth[1] += 1
        @debug "WLOCKED $(Symbol((lock_id,))) $(write_depth[1]) $(join(what, " ")) {{{"
    else
        read_key = Symbol((lock_id, false))
        @assert !haskey(private_storage, read_key)
        private_storage[write_key] = [1]
        @debug "WLOCK $(Symbol((lock_id,))) 1 $(join(what, " ")) {{{"
        lock(query_read_write_lock.lock)
        @debug "WLOCKED $(Symbol((lock_id,))) 1 $(join(what, " "))"
    end

    return nothing
end

"""
    write_unlock(query_read_write_lock::ReentrantReadWriteLock, what::Any)::Nothing

Release a write lock. Each call must matched a call to [`write_lock`](@ref). It is possible to nest
`write_lock`/`write_unlock` call pairs.

The log messages includes `what` is being unlocked.
"""
function write_unlock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    @assert haskey(private_storage, write_key)

    write_depth = private_storage[write_key]
    if write_depth[1] > 1
        write_depth[1] -= 1
        @debug "WUNLOCKED $(Symbol((lock_id,))) $(write_depth[1]) $(join(what, " ")) }}}"
    else
        read_key = Symbol((lock_id, false))
        @assert !haskey(private_storage, read_key)
        @assert write_depth[1] == 1
        delete!(private_storage, write_key)
        unlock(query_read_write_lock.lock)
        @debug "WUNLOCKED $(Symbol((lock_id,))) 0 $(join(what, " ")) }}}"
    end
    return nothing
end

"""
    has_write_lock(query_read_write_lock::ReentrantReadWriteLock)::Bool

Return whether the current task has the write lock.
"""
function has_write_lock(query_read_write_lock::ReentrantReadWriteLock)::Bool
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    return haskey(private_storage, write_key)
end

"""
    read_lock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Bool

Obtain a read lock. Each call must be matched by [`read_unlock`](@ref). It is possible to nest `read_lock`/`read_unlock`
call pairs, even inside `write_lock`/`write_unlock` pair(s); however, you can't nest `write_lock`/`write_unlock` inside
a `read_lock`/`read_unlock` pair.

When a task has a read lock, no other task can have a write lock, but other tasks may also have a read lock.

The log messages includes `what` is being locked.

Returns whether this is the top-level read lock (as opposed to a nested one).
"""
function read_lock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Bool
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    read_key = Symbol((lock_id, false))

    read_depth = get(private_storage, read_key, nothing)
    if read_depth !== nothing
        read_depth[1] += 1
        @debug "RLOCKED $(Symbol((lock_id,))) $(read_depth[1]) $(join(what, " ")) {{{"
        return false
    else
        private_storage[read_key] = [1]
        if !haskey(private_storage, write_key)
            @debug "RLOCK $(Symbol((lock_id,))) 1 $(join(what, " ")) {{{"
            lock_read(query_read_write_lock.lock)
        end
        @debug "RLOCKED $(Symbol((lock_id,))) 1 $(join(what, " "))"

        is_top_read_lock = get(private_storage, :generic_locks_top_read_lock, nothing)
        if is_top_read_lock === nothing
            private_storage[:generic_locks_top_read_lock] = lock_id
            return true
        else
            return false
        end
    end
end

"""
    read_unlock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Nothing

Release a read lock. Each call must matched a call to [`read_lock`](@ref). It is possible to nest
`read_lock`/`read_unlock` call pairs.

The log messages includes `what` is being unlocked.
"""
function read_unlock(query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Nothing
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    write_key = Symbol((lock_id, true))
    read_key = Symbol((lock_id, false))
    @assert haskey(private_storage, read_key)

    read_depth = private_storage[read_key]
    if read_depth[1] > 1
        read_depth[1] -= 1
        @debug "RUNLOCKED $(Symbol((lock_id,))) $(read_depth[1]) $(join(what, " ")) }}}"
    else
        @assert read_depth[1] == 1
        delete!(private_storage, read_key)
        if !haskey(private_storage, write_key)
            unlock_read(query_read_write_lock.lock)
        end
        @debug "RUNLOCKED $(Symbol((lock_id,))) 0 $(join(what, " ")) }}}"

        is_top_read_lock = private_storage[:generic_locks_top_read_lock]
        if is_top_read_lock === lock_id
            delete!(private_storage, :generic_locks_top_read_lock)
        end
    end
    return nothing
end

"""
    has_read_lock(query_read_write_lock::ReentrantReadWriteLock; read_only::Bool = false)::Bool

Return whether the current task has a read lock or the write lock. If `read_only` is set, then this will only return
whether the current task as a read lock.
"""
function has_read_lock(query_read_write_lock::ReentrantReadWriteLock; read_only::Bool = false)::Bool
    private_storage = task_local_storage()
    lock_id = objectid(query_read_write_lock.lock)
    return haskey(private_storage, Symbol((lock_id, false))) ||
           (!read_only && haskey(private_storage, Symbol((lock_id, true))))
end

"""
    with_write_lock(action::Function, query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Any

Perform an `action` while holding a [`write_lock`](@ref) for the `query_read_write_lock`, return
its result and [`write_unlock`](@ref).
"""
function with_write_lock(action::Function, query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Any
    write_lock(query_read_write_lock, what...)
    try
        return action()
    finally
        write_unlock(query_read_write_lock, what...)
    end
end

"""
    with_read_lock(action::Function, query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Any

Perform an `action` while holding a [`read_lock`](@ref) for the `query_read_write_lock`, return
its result and [`read_unlock`](@ref).
"""
function with_read_lock(action::Function, query_read_write_lock::ReentrantReadWriteLock, what::Any...)::Any
    read_lock(query_read_write_lock, what...)
    try
        return action()
    finally
        read_unlock(query_read_write_lock, what...)
    end
end

end
