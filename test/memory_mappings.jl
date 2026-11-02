# Whether the file and offset that the registry records for the first byte of a mapped `array` hold its first bytes.
function is_mapped_from_its_file(array::AbstractArray)::Bool
    array = canonical_array(array)
    position = mapped_file_position(pointer(array))
    if position === nothing
        return false  # UNTESTED
    end
    path, offset = position
    n_bytes = min(sizeof(array), 64)
    file_bytes = open(path) do io
        seek(io, offset)
        return read(io, n_bytes)
    end
    return file_bytes == reinterpret(UInt8, vec(array))[1:n_bytes]
end

# Every flat format maps its vectors and matrices through `mmap_with_small_pages`, which records them.
nested_test("memory_mappings") do
    mktempdir() do path
        for (name, new_daf) in new_destinations(path)
            if endswith(name, "_flat")
                nested_test(name) do
                    daf = new_daf()
                    add_axis!(daf, "cell", ["c$(index)" for index in 1:1000])
                    add_axis!(daf, "gene", ["g$(index)" for index in 1:20])
                    set_vector!(daf, "cell", "score", Float64.(1:1000))
                    set_matrix!(daf, "cell", "gene", "UMIs", Float32.(reshape(1:20_000, 1000, 20)); relayout = false)
                    empty_cache!(daf)
                    @test is_mapped_from_its_file(get_vector(daf, "cell", "score"))
                    @test is_mapped_from_its_file(get_matrix(daf, "cell", "gene", "UMIs"; relayout = false))
                    return nothing
                end
            end
        end

        nested_test("zip_read_only") do
            zip_path = joinpath(path, "read_only.daf.zip")
            daf = ZipDaf(zip_path, "w+"; name = "zip_written!", packed = false)
            add_axis!(daf, "cell", ["c$(index)" for index in 1:1000])
            set_vector!(daf, "cell", "score", Float64.(1:1000))
            daf = ZipDaf(zip_path, "r"; name = "zip_read_only!")
            @test is_mapped_from_its_file(get_vector(daf, "cell", "score"))
            return nothing
        end
    end
end
