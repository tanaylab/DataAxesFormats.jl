function populate_zarr_convert_test_data!(daf::DafWriter)::Nothing
    set_scalar!(daf, "version", "v1")
    set_scalar!(daf, "answer", 42)

    add_axis!(daf, "cell", ["A", "B", "C"])
    add_axis!(daf, "gene", ["X", "Y", "Z", "W"])

    set_vector!(daf, "cell", "age", [10, 20, 30])
    set_vector!(daf, "cell", "weight", Float32[1.5, 2.5, 3.5])
    set_vector!(daf, "cell", "flag", Bool[1, 0, 1])
    set_vector!(daf, "gene", "marker", SparseVector([1, 0, 1, 0]))
    set_vector!(daf, "gene", "present", SparseVector(Bool[1, 0, 1, 0]))
    set_vector!(daf, "cell", "color", ["aa", "bb", "cc"])
    set_vector!(daf, "gene", "label", ["", "", "", "hi"])

    set_matrix!(daf, "cell", "gene", "UMIs", [1 2 3 4; 5 6 7 8; 9 10 11 12])
    set_matrix!(daf, "cell", "gene", "sparse", sparse_matrix_csc([1 0 3 0; 0 6 0 8; 9 0 0 12]); relayout = false)
    set_matrix!(daf, "cell", "gene", "present", sparse_matrix_csc(Bool[1 0 1 0; 0 1 0 1; 1 0 0 1]); relayout = false)
    set_matrix!(
        daf,
        "cell",
        "gene",
        "annotation",
        ["a" "b" "c" "d"; "e" "f" "g" "h"; "i" "j" "k" "l"];
        relayout = false,
    )
    return nothing
end

function verify_zarr_convert_test_data(daf::DafReader)::Nothing
    @test get_scalar(daf, "version") == "v1"
    @test get_scalar(daf, "answer") == 42

    @test axis_entries(daf, "cell") == ["A", "B", "C"]
    @test axis_entries(daf, "gene") == ["X", "Y", "Z", "W"]

    @test get_vector(daf, "cell", "age") == [10, 20, 30]
    @test get_vector(daf, "cell", "weight") == Float32[1.5, 2.5, 3.5]
    @test get_vector(daf, "cell", "flag") == Bool[1, 0, 1]
    @test get_vector(daf, "gene", "marker") == [1, 0, 1, 0]
    @test get_vector(daf, "gene", "present") == Bool[1, 0, 1, 0]
    @test get_vector(daf, "cell", "color") == ["aa", "bb", "cc"]
    @test get_vector(daf, "gene", "label") == ["", "", "", "hi"]

    @test get_matrix(daf, "cell", "gene", "UMIs") == [1 2 3 4; 5 6 7 8; 9 10 11 12]
    @test get_matrix(daf, "cell", "gene", "sparse") == [1 0 3 0; 0 6 0 8; 9 0 0 12]
    @test get_matrix(daf, "cell", "gene", "present") == Bool[1 0 1 0; 0 1 0 1; 1 0 0 1]
    @test get_matrix(daf, "cell", "gene", "annotation") == ["a" "b" "c" "d"; "e" "f" "g" "h"; "i" "j" "k" "l"]
    return nothing
end

function same_inode(left_path::AbstractString, right_path::AbstractString)::Bool  # FLAKY TESTED
    left_stat = stat(left_path)
    right_stat = stat(right_path)
    return left_stat.device == right_stat.device && left_stat.inode == right_stat.inode
end

nested_test("zarr_convert") do
    nested_test("files_to_zarr") do
        nested_test("round_trip") do
            mktempdir() do path
                files_src = "$(path)/src"
                zarr_mid = "$(path)/mid.daf.zarr"
                files_dst = "$(path)/dst"

                source = FilesDaf(files_src, "w"; name = "src!")
                populate_zarr_convert_test_data!(source)

                files_to_zarr(; files_path = files_src, zarr_path = zarr_mid)
                zarr_to_files(; zarr_path = zarr_mid, files_path = files_dst)

                destination = FilesDaf(files_dst, "r"; name = "dst!")
                verify_zarr_convert_test_data(destination)

                @test same_inode("$(files_src)/matrices/cell/gene/UMIs.data", "$(zarr_mid)/matrices/cell/gene/UMIs/0.0")
                @test same_inode(
                    "$(files_src)/matrices/cell/gene/UMIs.data",
                    "$(files_dst)/matrices/cell/gene/UMIs.data",
                )
                @test same_inode(
                    "$(files_src)/matrices/cell/gene/sparse.nzval",
                    "$(zarr_mid)/matrices/cell/gene/sparse/nzval/0",
                )
                @test same_inode("$(files_src)/vectors/gene/marker.nzind", "$(zarr_mid)/vectors/gene/marker/nzind/0")
                @test !isfile("$(zarr_mid)/vectors/gene/present/nzval/0")
                @test !isfile("$(zarr_mid)/matrices/cell/gene/present/nzval/0")
                return nothing
            end
        end

        nested_test("errors") do
            nested_test("bad_destination_name") do
                mktempdir() do path
                    files_src = "$(path)/src"
                    FilesDaf(files_src, "w"; name = "src!")
                    @test_throws "ZarrDaf directory path must end with .daf.zarr: $(path)/bad.zarr" files_to_zarr(;
                        files_path = files_src,
                        zarr_path = "$(path)/bad.zarr",
                    )
                end
            end

            nested_test("destination_exists") do
                mktempdir() do path
                    files_src = "$(path)/src"
                    FilesDaf(files_src, "w"; name = "src!")
                    zarr_dst = "$(path)/dst.daf.zarr"
                    mkdir(zarr_dst)
                    @test_throws "destination already exists: $(zarr_dst)" files_to_zarr(;
                        files_path = files_src,
                        zarr_path = zarr_dst,
                    )
                end
            end

            nested_test("bad_source") do
                mktempdir() do path
                    files_src = "$(path)/src"
                    mkdir(files_src)
                    @test_throws "not a daf files directory: $(files_src)" files_to_zarr(;
                        files_path = files_src,
                        zarr_path = "$(path)/dst.daf.zarr",
                    )
                end
            end

            nested_test("missing_source") do
                mktempdir() do path
                    files_src = "$(path)/missing"
                    @test_throws "not a directory: $(files_src)" files_to_zarr(;
                        files_path = files_src,
                        zarr_path = "$(path)/dst.daf.zarr",
                    )
                end
            end

            nested_test("zip_destination") do
                mktempdir() do path
                    files_src = "$(path)/src"
                    FilesDaf(files_src, "w"; name = "src!")
                    @test_throws "can't convert into a zip-backed ZarrDaf: $(path)/dst.daf.zarr.zip" files_to_zarr(;
                        files_path = files_src,
                        zarr_path = "$(path)/dst.daf.zarr.zip",
                    )
                end
            end

            nested_test("http_destination") do
                mktempdir() do path
                    files_src = "$(path)/src"
                    FilesDaf(files_src, "w"; name = "src!")
                    @test_throws "can't convert into a remote ZarrDaf over HTTP: http://example.com/dst.daf.zarr" files_to_zarr(;
                        files_path = files_src,
                        zarr_path = "http://example.com/dst.daf.zarr",
                    )
                end
            end
        end
    end

    nested_test("zarr_to_files") do
        nested_test("round_trip") do
            mktempdir() do path
                zarr_src = "$(path)/src.daf.zarr"
                files_mid = "$(path)/mid"
                zarr_dst = "$(path)/dst.daf.zarr"

                source = ZarrDaf(zarr_src, "w"; name = "src!")
                populate_zarr_convert_test_data!(source)

                zarr_to_files(; zarr_path = zarr_src, files_path = files_mid)
                files_to_zarr(; files_path = files_mid, zarr_path = zarr_dst)

                destination = ZarrDaf(zarr_dst, "r"; name = "dst!")
                verify_zarr_convert_test_data(destination)

                @test same_inode("$(zarr_src)/matrices/cell/gene/UMIs/0.0", "$(files_mid)/matrices/cell/gene/UMIs.data")
                @test same_inode(
                    "$(zarr_src)/matrices/cell/gene/sparse/colptr/0",
                    "$(files_mid)/matrices/cell/gene/sparse.colptr",
                )
                @test !isfile("$(files_mid)/matrices/cell/gene/present.nzval")
                return nothing
            end
        end

        nested_test("errors") do
            nested_test("bad_source_name") do
                mktempdir() do path
                    @test_throws "ZarrDaf directory path must end with .daf.zarr: $(path)/bad.zarr" zarr_to_files(;
                        zarr_path = "$(path)/bad.zarr",
                        files_path = "$(path)/dst",
                    )
                end
            end

            nested_test("http_source") do
                @test_throws "can't convert a remote ZarrDaf over HTTP: http://example.com/foo.daf.zarr" zarr_to_files(;
                    zarr_path = "http://example.com/foo.daf.zarr",
                    files_path = "/tmp/daf_convert_test_dst",
                )
            end

            nested_test("missing_source") do
                mktempdir() do path
                    @test_throws "not a directory: $(path)/missing.daf.zarr" zarr_to_files(;
                        zarr_path = "$(path)/missing.daf.zarr",
                        files_path = "$(path)/dst",
                    )
                end
            end

            nested_test("zip_source") do
                mktempdir() do path
                    @test_throws "can't convert a zip-backed ZarrDaf: $(path)/src.daf.zarr.zip" zarr_to_files(;
                        zarr_path = "$(path)/src.daf.zarr.zip",
                        files_path = "$(path)/dst",
                    )
                end
            end

            nested_test("not_a_daf_zarr") do
                mktempdir() do path
                    zarr_src = "$(path)/src.daf.zarr"
                    mkdir(zarr_src)
                    @test_throws "not a daf zarr directory: $(zarr_src)" zarr_to_files(;
                        zarr_path = zarr_src,
                        files_path = "$(path)/dst",
                    )
                end
            end

            nested_test("destination_exists") do
                mktempdir() do path
                    zarr_src = "$(path)/src.daf.zarr"
                    ZarrDaf(zarr_src, "w"; name = "src!")
                    files_dst = "$(path)/dst"
                    mkdir(files_dst)
                    @test_throws "destination already exists: $(files_dst)" zarr_to_files(;
                        zarr_path = zarr_src,
                        files_path = files_dst,
                    )
                end
            end
        end
    end
end
