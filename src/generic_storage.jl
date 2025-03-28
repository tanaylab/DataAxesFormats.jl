"""
Allow reusing large data between parallel tasks, which arguably should belong in a more general-purpose package.

!!! note

    We do not re-export the types and functions defined here from the top-level `DataAxesFormats` namespace. That is,
    even if `using DataAxesFormats`, you will **not** have these generic names polluting your namespace. If you do want
    to reuse them in your code, explicitly write `using DataAxesFormats.GenericTypes`.

Using task local storage will re-allocate and re-initialize this data for each iteration, which is slow. Using thread
local storage is no longer safe because Julia has moved away from "sticky" threads (that is, a task may migrate between
threads); if naively implemented, it also create an instance per thread regardless whether it is actually used. The
[`ReusableStorage`](@ref) allows allocating the minimal number of instances, reusing them in multiple tasks, and
automates resetting the data each time it is used by a new task.
"""
module GenericStorage

export ReusableStorage
export get_reusable!
export put_reusable!
export with_reusable

using Base.Threads
using ..GenericTypes

"""
    ReusableStorage(create::Function)::ReusableStorage

Implement resuable storage for parallel computations. This allocates and reuses the minimal number of instances of some
data for (re)use by parallel tasks. The `create` function should return a new instance of the data. If a previous task
has used the data, we will `reset` it to bring it back it to its initial state.

!!! note

    The `create` and `reset` functions must be thread safe. We intentionally do not
"""
mutable struct ReusableStorage{T}
    lock::SpinLock
    data::Vector{T}
    create::Function
    reset::Maybe{Function}
    is_reset::Bool
end

function ReusableStorage(create::Function, reset::Maybe{Function} = nothing)::ReusableStorage
    return ReusableStorage(SpinLock(), [create()], create, reset, true)
end

"""
    get_reusable!(reusable_storage::ReusableStorage{T})::T where{T}

Get a private instance of the data from the reusable storage. Will prefer to return an existing instance of the data,
after being `reset` if used by a previous task. If all instances are currently being used by other tasks, will `create`
a new instance instead.
"""
function get_reusable!(reusable_storage::ReusableStorage{T})::T where {T}
    data, create, reset = lock(reusable_storage.lock) do
        if reusable_storage.is_reset
            @assert length(reusable_storage.data) == 1
            reusable_storage.is_reset = false
            return (pop!(reusable_storage.data), false, false)

        elseif isempty(reusable_storage.data)
            return (nothing, true, false)

        else
            return (pop!(reusable_storage.data), false, true)
        end
    end

    if create
        @assert data === nothing
        data = reusable_storage.create()
    end

    if reset && reusable_storage.reset !== nothing
        reusable_storage.reset(data)
    end

    return data
end

"""
    put_reusable!(reusable_storage::ReusableStorage{T}, data::T)::Nothing

Put back data into the `reusable_storage` once a task is done with it. This will allow other tasks to reuse this data
(after being `reset`).
"""
function put_reusable!(reusable_storage::ReusableStorage{T}, data::T)::Nothing where {T}
    lock(reusable_storage.lock) do
        push!(reusable_storage.data, data)
        return nothing
    end
    return nothing
end

"""
    with_reusable(action::Function, reusable_storage::ReusableStorage)::Any

Invoke the `action` function with data fetched from the `reusable_storage`. Returns whatever the `action` returns. This
just pairs [`get_reusable!`](@ref) and [`put_reusable!`](@ref), even in the presence of exceptions.
"""
function with_reusable(action::Function, reusable_storage::ReusableStorage)::Any
    data = get_reusable!(reusable_storage)
    try
        return action(data)
    finally
        put_reusable!(reusable_storage, data)
    end
end

end
