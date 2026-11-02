# Whether the memory of the `array` is mapped from a file with small pages: its mapping in `/proc/self/smaps` names a
# file and has the `nh` (no huge pages) flag.
function is_mapped_with_small_pages(array::AbstractArray)::Bool
    address = UInt(pointer(canonical_array(array)))
    is_in_file_mapping = false
    for line in eachline("/proc/self/smaps")
        range_match = match(r"^([0-9a-f]+)-([0-9a-f]+) \S+ \S+ \S+ \S+\s*(\S*)", line)
        if range_match !== nothing
            is_in_file_mapping =
                parse(UInt, range_match[1]; base = 16) <= address < parse(UInt, range_match[2]; base = 16) &&
                !isempty(range_match[3])
        elseif is_in_file_mapping && startswith(line, "VmFlags:")
            return occursin(" nh", line)
        end
    end
    return false  # UNTESTED
end

# Every flat format maps its vectors and matrices through `mmap_with_small_pages`.
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
                    @test is_mapped_with_small_pages(get_vector(daf, "cell", "score"))
                    @test is_mapped_with_small_pages(get_matrix(daf, "cell", "gene", "UMIs"; relayout = false))
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
            @test is_mapped_with_small_pages(get_vector(daf, "cell", "score"))
            return nothing
        end
    end
end
