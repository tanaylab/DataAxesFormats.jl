CELL_NAMES = ["TATA", "GATA", "CATA"]
CELL_TYPES_BY_DEPTH =
    [["Bcell", "TCell", "TCell"], ["TCell", "Bcell", "TCell"], ["TCell", "TCell", "BCell"], ["BCell", "BCell", "TCell"]]
GENE_NAMES = ["RSPO3", "FOXA1", "WNT6", "TNNI1"]
MARKER_GENES_BY_DEPTH =
    [[true, false, true, false], [false, true, false, true], [false, false, true, true], [true, true, false, false]]
UMIS_BY_DEPTH =
    [[0 1 2 3; 1 2 3 0; 2 3 0 1], [1 2 3 0; 2 3 0 1; 3 0 1 2], [2 3 0 1; 3 0 1 2; 0 1 2 3], [3 0 1 2; 0 1 2 3; 1 2 3 0]]

function test_missing_scalar(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_missing_scalar(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_scalar") do
        @test !has_scalar(daf, "depth")
        @test !has_scalar(daf, "version")
    end

    nested_test("scalars_set") do
        @test isempty(scalars_set(daf))
    end

    nested_test("get_scalar") do
        nested_test("()") do
            @test_throws dedent("""
                missing scalar: depth
                in the daf data: $(daf.name)
            """) get_scalar(daf, "depth")
            @test_throws dedent("""
                missing scalar: version
                in the daf data: $(daf.name)
            """) get_scalar(daf, "version")
        end

        nested_test("default") do
            nested_test("missing") do
                @test get_scalar(daf, "depth"; default = nothing) === nothing
                @test get_scalar(daf, "version"; default = nothing) === nothing
            end

            nested_test("scalar") do
                @test get_scalar(daf, "depth"; default = -2) == -2
                @test get_scalar(daf, "version"; default = 0) == 0
            end
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_scalar!") do
        nested_test("()") do
            @test_throws dedent("""
                missing scalar: depth
                in the daf data: $(daf.name)
            """) delete_scalar!(daf, "depth")
            @test_throws dedent("""
                missing scalar: version
                in the daf data: $(daf.name)
            """) delete_scalar!(daf, "version")
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test_throws dedent("""
                    missing scalar: depth
                    in the daf data: $(daf.name)
                """) delete_scalar!(daf, "depth"; must_exist = true)
                @test_throws dedent("""
                    missing scalar: version
                    in the daf data: $(daf.name)
                """) delete_scalar!(daf, "version"; must_exist = true)
            end

            nested_test("false") do
                @test delete_scalar!(daf, "depth"; must_exist = false) === nothing
                @test delete_scalar!(daf, "version"; must_exist = false) === nothing
            end
        end
    end

    nested_test("set_scalar!") do
        @test set_scalar!(daf, "depth", depth + 1) === nothing
        @test set_scalar!(daf, "version", "1.0") === nothing
        empty_cache!(daf)
        nested_test("created") do
            test_existing_scalar(daf, depth + 1)
            return nothing
        end
    end
end

function test_existing_scalar(daf::DafReader, depth::Int)::Nothing
    if daf isa DafWriter
        nested_test("read_only") do
            test_existing_scalar(read_only(daf), depth)
            return nothing
        end
    end

    if depth > 2
        return nothing
    end

    nested_test("has_scalar") do
        @test has_scalar(daf, "depth")
        @test has_scalar(daf, "version")
    end

    nested_test("scalars_set") do
        @test scalars_set(daf) == Set(["depth", "version"])
    end

    nested_test("get_scalar") do
        nested_test("()") do
            @test get_scalar(daf, "depth") == depth
            @test get_scalar(daf, "version") == "1.0"
        end

        nested_test("default") do
            @test get_scalar(daf, "depth"; default = -2) == depth
            @test get_scalar(daf, "version"; default = -2) == "1.0"
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_scalar!") do
        nested_test("()") do
            @test delete_scalar!(daf, "depth") === nothing
            @test delete_scalar!(daf, "version") === nothing
            nested_test("deleted") do
                test_missing_scalar(daf, depth + 1)
                return nothing
            end
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test delete_scalar!(daf, "depth"; must_exist = true) === nothing
                @test delete_scalar!(daf, "version"; must_exist = true) === nothing
                nested_test("deleted") do
                    test_missing_scalar(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test delete_scalar!(daf, "depth"; must_exist = false) === nothing
                @test delete_scalar!(daf, "version"; must_exist = false) === nothing
                nested_test("deleted") do
                    test_missing_scalar(daf, depth + 1)
                    return nothing
                end
            end
        end
    end

    nested_test("set_scalar!") do
        nested_test("()") do
            @test_throws dedent("""
                existing scalar: depth
                in the daf data: $(daf.name)
            """) set_scalar!(daf, "depth", -1)
            @test_throws dedent("""
                existing scalar: version
                in the daf data: $(daf.name)
            """) set_scalar!(daf, "version", -1)
        end

        nested_test("overwrite") do
            nested_test("false") do
                @test_throws dedent("""
                    existing scalar: depth
                    in the daf data: $(daf.name)
                """) set_scalar!(daf, "depth", -1; overwrite = false)
                @test_throws dedent("""
                    existing scalar: version
                    in the daf data: $(daf.name)
                """) set_scalar!(daf, "version", -1; overwrite = false)
            end

            nested_test("true") do
                @test set_scalar!(daf, "depth", depth + 1; overwrite = true) === nothing
                @test set_scalar!(daf, "version", "1.0"; overwrite = true) === nothing
                nested_test("overwritten") do
                    test_existing_scalar(daf, depth + 1)
                    return nothing
                end
            end
        end
    end
end

function test_missing_axis(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_missing_axis(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_axis") do
        @test !has_axis(daf, "gene")
    end

    nested_test("axis_length") do
        @test_throws dedent("""
            missing axis: gene
            for: axis_length
            of the daf data: $(daf.name)
        """) axis_length(daf, "gene")
    end

    nested_test("axes_set") do
        @test isempty(axes_set(daf))
    end

    nested_test("vectors_set") do
        @test_throws dedent("""
            missing axis: gene
            for: vectors_set
            of the daf data: $(daf.name)
        """) vectors_set(daf, "gene")
    end

    nested_test("axis_vector") do
        nested_test("()") do
            @test_throws dedent("""
                missing axis: gene
                for: axis_vector
                of the daf data: $(daf.name)
            """) axis_vector(daf, "gene")
        end

        nested_test("default") do
            nested_test("undef") do
                @test_throws dedent("""
                    missing axis: gene
                    for: axis_vector
                    of the daf data: $(daf.name)
                """) axis_vector(daf, "gene"; default = undef)
            end

            nested_test("nothing") do
                @test axis_vector(daf, "gene"; default = nothing) === nothing
            end
        end
    end

    nested_test("axis_dict") do
        @test_throws dedent("""
            missing axis: gene
            for: axis_dict
            of the daf data: $(daf.name)
        """) axis_dict(daf, "gene")
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_axis") do
        nested_test("()") do
            @test_throws dedent("""
                missing axis: gene
                for: delete_axis!
                of the daf data: $(daf.name)
            """) delete_axis!(daf, "gene")
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test_throws dedent("""
                    missing axis: gene
                    for: delete_axis!
                    of the daf data: $(daf.name)
                """) delete_axis!(daf, "gene"; must_exist = true)
            end

            nested_test("false") do
                @test delete_axis!(daf, "gene"; must_exist = false) === nothing
            end
        end
    end

    nested_test("add_axis") do
        nested_test("unique") do
            @test add_axis!(daf, "gene", GENE_NAMES) === nothing
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test axis_version_counter(daf, "gene") == depth - 1
            @test axis_version_counter(daf, "cell") == depth - 1
            nested_test("created") do
                test_existing_axis(daf, depth + 1)
                return nothing
            end
        end

        nested_test("duplicated") do
            @test_throws dedent("""
                non-unique entries for new axis: gene
                in the daf data: $(daf.name)
            """) add_axis!(daf, "gene", ["FOXA1", "BATF3", "CATD2", "BATF3"])
        end
    end
end

function test_existing_axis(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing  # untested
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_existing_axis(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_axis") do
        @test has_axis(daf, "gene")
    end

    nested_test("axis_length") do
        @test axis_length(daf, "gene") == length(GENE_NAMES)
    end

    nested_test("vectors_set") do
        @test isempty(vectors_set(daf, "gene"))
    end

    nested_test("axes_set") do
        @test axes_set(daf) == Set(["gene", "cell"])
    end

    nested_test("axis_vector") do
        @test axis_vector(daf, "gene") == GENE_NAMES
        @test axis_vector(daf, "cell") == CELL_NAMES
    end

    nested_test("axis_entries") do
        @test axis_entries(daf, "gene", 1:length(GENE_NAMES)) == GENE_NAMES
        @test axis_entries(daf, "cell", [0]; allow_empty = true) == [""]
        @test axis_entries(daf, "cell") == axis_vector(daf, "cell")
    end

    nested_test("axis_dict") do
        @test collect(axis_dict(daf, "gene")) == [name => index for (index, name) in enumerate(GENE_NAMES)]
        @test collect(axis_dict(daf, "cell")) == [name => index for (index, name) in enumerate(CELL_NAMES)]
    end

    nested_test("axis_indices") do
        @test axis_indices(daf, "gene", GENE_NAMES) == collect(1:length(GENE_NAMES))
        @test axis_indices(daf, "cell", reverse(CELL_NAMES)) == collect(reverse(1:length(CELL_NAMES)))
        @test axis_indices(daf, "cell", [""]; allow_empty = true) == [0]
    end

    nested_test("name") do
        @test get_vector(daf, "gene", "name") == GENE_NAMES
        @test get_vector(daf, "cell", "name") == CELL_NAMES
    end

    nested_test("index") do
        @test get_vector(daf, "gene", "index") == collect(1:length(GENE_NAMES))
        @test get_vector(daf, "cell", "index") == collect(1:length(CELL_NAMES))
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_axis!") do
        nested_test("()") do
            @test delete_axis!(daf, "gene") === nothing
            @test delete_axis!(daf, "cell") === nothing
            nested_test("deleted") do
                test_missing_axis(daf, depth + 1)
                return nothing
            end
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test delete_axis!(daf, "gene"; must_exist = true) === nothing
                @test delete_axis!(daf, "cell"; must_exist = true) === nothing
                nested_test("deleted") do
                    test_missing_axis(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test delete_axis!(daf, "gene"; must_exist = false) === nothing
                @test delete_axis!(daf, "cell"; must_exist = false) === nothing
                nested_test("deleted") do
                    test_missing_axis(daf, depth + 1)
                    return nothing
                end
            end
        end
    end

    nested_test("add_axis!") do
        nested_test("()") do
            @test_throws dedent("""
                existing axis: gene
                in the daf data: $(daf.name)
            """) add_axis!(daf, "gene", ["Foo", "Bar", "Baz"])
        end

        nested_test("overwrite") do
            add_axis!(daf, "gene", ["Foo", "Bar", "Baz"]; overwrite = true)
            @test axis_vector(daf, "gene") == ["Foo", "Bar", "Baz"]
        end
    end
end

function test_missing_vector_axis(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_missing_vector_axis(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_vector") do
        @test_throws dedent("""
            missing axis: gene
            for has_vector: marker
            of the daf data: $(daf.name)
        """) has_vector(daf, "gene", "marker")
    end

    nested_test("vectors_set") do
        @test_throws dedent("""
            missing axis: gene
            for: vectors_set
            of the daf data: $(daf.name)
        """) vectors_set(daf, "gene")
    end

    nested_test("get_vector") do
        @test_throws dedent("""
            missing axis: gene
            for the vector: marker
            of the daf data: $(daf.name)
        """) get_vector(daf, "gene", "marker")
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_vector") do
        nested_test("()") do
            @test_throws dedent("""
                missing axis: gene
                for the vector: marker
                of the daf data: $(daf.name)
            """) delete_vector!(daf, "gene", "marker")
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test_throws dedent("""
                    missing axis: gene
                    for the vector: marker
                    of the daf data: $(daf.name)
                """) delete_vector!(daf, "gene", "marker"; must_exist = true)
            end

            nested_test("false") do
                @test_throws dedent("""
                    missing axis: gene
                    for the vector: marker
                    of the daf data: $(daf.name)
                """) delete_vector!(daf, "gene", "marker"; must_exist = false)
            end
        end
    end

    nested_test("set_vector!") do
        nested_test("()") do
            @test_throws dedent("""
                missing axis: gene
                for the vector: marker
                of the daf data: $(daf.name)
            """) set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth])
        end

        nested_test("overwrite") do
            nested_test("true") do
                @test_throws dedent("""
                    missing axis: gene
                    for the vector: marker
                    of the daf data: $(daf.name)
                """) set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth]; overwrite = false)
            end

            nested_test("false") do
                @test_throws dedent("""
                    missing axis: gene
                    for the vector: marker
                    of the daf data: $(daf.name)
                """) set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth]; overwrite = true)
            end
        end
    end
end

function test_missing_vector(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_missing_vector(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_vector") do
        @test !has_vector(daf, "gene", "marker")
    end

    nested_test("vectors_set") do
        @test isempty(vectors_set(daf, "gene"))
    end

    nested_test("get_vector") do
        nested_test("()") do
            @test_throws dedent("""
                missing vector: marker
                for the axis: gene
                in the daf data: $(daf.name)
            """) get_vector(daf, "gene", "marker")
        end

        nested_test("default") do
            nested_test("undef") do
                @test_throws dedent("""
                    missing vector: marker
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) get_vector(daf, "gene", "marker"; default = undef)
            end

            nested_test("nothing") do
                @test get_vector(daf, "gene", "marker"; default = nothing) === nothing
            end

            nested_test("scalar") do
                @test get_vector(daf, "gene", "marker"; default = 1) == [1, 1, 1, 1]
                @test get_vector(daf, "gene", "marker"; default = 0) == [0, 0, 0, 0]
                @test dimnames(get_vector(daf, "gene", "marker"; default = 1)) == ["gene"]
                @test names(get_vector(daf, "gene", "marker"; default = 0)) == [GENE_NAMES]
            end

            nested_test("vector") do
                @test get_vector(daf, "gene", "marker"; default = [true, true, false, false]) ==
                      [true, true, false, false]
                @test dimnames(get_vector(daf, "gene", "marker"; default = [true, true, false, false])) == ["gene"]
                @test names(get_vector(daf, "gene", "marker"; default = [true, true, false, false])) == [GENE_NAMES]
            end

            nested_test("named") do
                nested_test("()") do
                    @test get_vector(
                        daf,
                        "gene",
                        "marker";
                        default = NamedArray([true, true, false, false]; names = (GENE_NAMES,), dimnames = ("gene",)),
                    ) == [true, true, false, false]
                    @test dimnames(
                        get_vector(
                            daf,
                            "gene",
                            "marker";
                            default = NamedArray(
                                [true, true, false, false];
                                names = (GENE_NAMES,),
                                dimnames = ("gene",),
                            ),
                        ),
                    ) == ["gene"]
                    @test names(
                        get_vector(
                            daf,
                            "gene",
                            "marker";
                            default = NamedArray(
                                [true, true, false, false];
                                names = (GENE_NAMES,),
                                dimnames = ("gene",),
                            ),
                        ),
                    ) == [GENE_NAMES]
                end

                nested_test("!dim") do
                    @test_throws dedent("""
                        default dim name: A
                        is different from the axis: gene
                        in the daf data: $(daf.name)
                    """) get_vector(
                        daf,
                        "gene",
                        "marker",
                        default = NamedArray([true, true, false, false]; names = (GENE_NAMES,)),
                    )
                end

                nested_test("!names") do
                    @test_throws dedent("""
                        entry names of the: default
                        mismatch the entry names of the axis: gene
                        in the daf data: $(daf.name)
                    """) get_vector(
                        daf,
                        "gene",
                        "marker";
                        default = NamedArray(
                            MARKER_GENES_BY_DEPTH[depth];
                            names = (["A", "B", "C", "D"],),
                            dimnames = ("gene",),
                        ),
                    ) === nothing
                end
            end
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_vector") do
        nested_test("()") do
            @test_throws dedent("""
                missing vector: marker
                for the axis: gene
                in the daf data: $(daf.name)
            """) delete_vector!(daf, "gene", "marker")
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test_throws dedent("""
                    missing vector: marker
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) delete_vector!(daf, "gene", "marker"; must_exist = true)
            end

            nested_test("false") do
                @test delete_vector!(daf, "gene", "marker"; must_exist = false) === nothing
                @test delete_vector!(daf, "cell", "type"; must_exist = false) === nothing
            end
        end
    end

    nested_test("set_vector!") do
        nested_test("scalar") do
            @test set_vector!(daf, "gene", "marker", 1.0) === nothing
            @test get_vector(daf, "gene", "marker") == [1.0, 1.0, 1.0, 1.0]
            @test set_vector!(daf, "gene", "noisy", 0.0; eltype = Int32) === nothing
            @test get_vector(daf, "gene", "noisy") == [0.0, 0.0, 0.0, 0.0]
            @test eltype(get_vector(daf, "gene", "noisy")) == Int32
            @test set_vector!(daf, "cell", "type", "TCell") === nothing
            @test get_vector(daf, "cell", "type") == ["TCell", "TCell", "TCell"]
        end

        nested_test("vector") do
            nested_test("dense") do
                nested_test("exists") do
                    @test set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth]) === nothing
                    @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]) === nothing
                    test_existing_vector(daf, depth + 1)
                    return nothing
                end

                nested_test("eltype") do
                    @test set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth]; eltype = Float32) === nothing
                    @test eltype(get_vector(daf, "gene", "marker")) == Float32
                    return nothing
                end
            end

            nested_test("sparse") do
                nested_test("exists") do
                    @test set_vector!(daf, "gene", "marker", sparse_vector(MARKER_GENES_BY_DEPTH[depth])) === nothing
                    # TODO: When SparseArrays supports strings, test it.
                    @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]) === nothing
                    test_existing_vector(daf, depth + 1)
                    return nothing
                end

                nested_test("eltype") do
                    @test set_vector!(
                        daf,
                        "gene",
                        "marker",
                        sparse_vector(MARKER_GENES_BY_DEPTH[depth]);
                        eltype = Float32,
                    ) === nothing
                    @test eltype(get_vector(daf, "gene", "marker")) == Float32
                    return nothing
                end
            end

            nested_test("name") do
                @test_throws dedent("""
                    setting the reserved vector: name
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) set_vector!(daf, "gene", "name", 1)
            end

            nested_test("index") do
                @test_throws dedent("""
                    setting the reserved vector: index
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) set_vector!(daf, "gene", "index", 1)
            end

            nested_test("!size") do
                @test_throws dedent("""
                    the length: 3
                    of the vector: marker
                    is different from the length: 4
                    of the axis: gene
                    in the daf data: $(daf.name)
                """) set_vector!(daf, "gene", "marker", [true, true, true])
            end

            nested_test("named") do
                nested_test("()") do
                    @test set_vector!(
                        daf,
                        "gene",
                        "marker",
                        NamedArray(MARKER_GENES_BY_DEPTH[depth]; names = (GENE_NAMES,), dimnames = ("gene",)),
                    ) === nothing
                    @test set_vector!(
                        daf,
                        "cell",
                        "type",
                        NamedArray(CELL_TYPES_BY_DEPTH[depth]; names = (CELL_NAMES,), dimnames = ("cell",)),
                    ) === nothing
                    test_existing_vector(daf, depth + 1)
                    return nothing
                end

                nested_test("!dim") do
                    @test_throws dedent("""
                        vector dim name: A
                        is different from the axis: gene
                        in the daf data: $(daf.name)
                    """) set_vector!(
                        daf,
                        "gene",
                        "marker",
                        NamedArray(MARKER_GENES_BY_DEPTH[depth]; names = (GENE_NAMES,)),
                    )
                end

                nested_test("!names") do
                    @test_throws dedent("""
                        entry names of the: vector
                        mismatch the entry names of the axis: gene
                        in the daf data: $(daf.name)
                    """) set_vector!(
                        daf,
                        "gene",
                        "marker",
                        NamedArray(MARKER_GENES_BY_DEPTH[depth]; names = (["A", "B", "C", "D"],), dimnames = ("gene",)),
                    ) === nothing
                end
            end
        end
    end

    nested_test("empty_vector!") do
        nested_test("dense") do
            previous_version_counter = vector_version_counter(daf, "gene", "marker")
            @test empty_dense_vector!(daf, "gene", "marker", Bool) do empty_vector
                empty_vector .= MARKER_GENES_BY_DEPTH[depth]
                return 7
            end == 7
            @test vector_version_counter(daf, "gene", "marker") == previous_version_counter + 1

            previous_version_counter = vector_version_counter(daf, "cell", "type")
            @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]; overwrite = true) === nothing
            @test vector_version_counter(daf, "gene", "marker") == previous_version_counter + 1

            test_existing_vector(daf, depth + 1)
            return nothing
        end

        nested_test("sparse") do
            previous_version_counter = vector_version_counter(daf, "gene", "marker")
            @test empty_sparse_vector!(
                daf,
                "gene",
                "marker",
                Bool,
                sum(MARKER_GENES_BY_DEPTH[depth]),
                Int32,
            ) do empty_nzind, empty_nzval
                sparse = sparse_vector(MARKER_GENES_BY_DEPTH[depth])
                empty_nzind .= sparse.nzind
                empty_nzval .= sparse.nzval
                return 7
            end == 7

            @test vector_version_counter(daf, "gene", "marker") == previous_version_counter + 1

            previous_version_counter = vector_version_counter(daf, "cell", "type")
            @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]; overwrite = true) === nothing
            @test vector_version_counter(daf, "gene", "marker") == previous_version_counter + 1

            test_existing_vector(daf, depth + 1)
            return nothing
        end
    end
end

function test_existing_vector(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_existing_vector(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_vector") do
        @test has_vector(daf, "gene", "marker")
    end

    nested_test("vectors_set") do
        @test vectors_set(daf, "gene") == Set(["marker"])
    end

    nested_test("get_vector") do
        nested_test("()") do
            @test get_vector(daf, "gene", "marker") == MARKER_GENES_BY_DEPTH[depth - 1]
            @test dimnames(get_vector(daf, "gene", "marker")) == ["gene"]
            @test names(get_vector(daf, "gene", "marker")) == [GENE_NAMES]
            @test get_vector(daf, "cell", "type") == CELL_TYPES_BY_DEPTH[depth - 1]
            @test dimnames(get_vector(daf, "cell", "type")) == ["cell"]
            @test names(get_vector(daf, "cell", "type")) == [CELL_NAMES]
        end

        nested_test("default") do
            nested_test("()") do
                @test get_vector(daf, "gene", "marker"; default = [false, false, false, false]) ==
                      MARKER_GENES_BY_DEPTH[depth - 1]
                @test dimnames(get_vector(daf, "gene", "marker"; default = [false, false, false, false])) == ["gene"]
                @test names(get_vector(daf, "gene", "marker"; default = [false, false, false, false])) == [GENE_NAMES]
            end

            nested_test("!size") do
                @test_throws dedent("""
                    the length: 3
                    of the default for the vector: marker
                    is different from the length: 4
                    of the axis: gene
                    in the daf data: $(daf.name)
                """) get_vector(daf, "gene", "marker"; default = [false, false, false])
            end
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_vector!") do
        nested_test("()") do
            @test delete_vector!(daf, "gene", "marker") === nothing
            @test delete_vector!(daf, "cell", "type") === nothing
            nested_test("deleted") do
                test_missing_vector(daf, depth + 1)
                return nothing
            end
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test delete_vector!(daf, "gene", "marker"; must_exist = true) === nothing
                @test delete_vector!(daf, "cell", "type"; must_exist = true) === nothing
                nested_test("deleted") do
                    test_missing_vector(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test delete_vector!(daf, "gene", "marker"; must_exist = false) === nothing
                @test delete_vector!(daf, "cell", "type"; must_exist = false) === nothing
                nested_test("deleted") do
                    test_missing_vector(daf, depth + 1)
                    return nothing
                end
            end
        end
    end

    nested_test("delete_axis!") do
        @test delete_axis!(daf, "gene") === nothing
        test_missing_vector_axis(daf, depth + 1)
        return nothing
    end

    nested_test("set_vector!") do
        nested_test("()") do
            @test_throws dedent("""
                existing vector: marker
                for the axis: gene
                in the daf data: $(daf.name)
            """) set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth])
        end

        nested_test("overwrite") do
            nested_test("false") do
                @test_throws dedent("""
                    existing vector: marker
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth]; overwrite = false)
            end

            nested_test("true") do
                @test set_vector!(daf, "gene", "marker", MARKER_GENES_BY_DEPTH[depth]; overwrite = true) === nothing
                @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]; overwrite = true) === nothing
                nested_test("overwritten") do
                    test_existing_vector(daf, depth + 1)
                    return nothing
                end
            end
        end
    end

    nested_test("empty_vector!") do
        nested_test("dense") do
            nested_test("()") do
                return @test_throws dedent("""
                    existing vector: marker
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) empty_dense_vector!(daf, "gene", "marker", Bool) do empty_vector
                    @assert false
                end
            end

            nested_test("overwrite") do
                nested_test("false") do
                    return @test_throws dedent("""
                        existing vector: marker
                        for the axis: gene
                        in the daf data: $(daf.name)
                    """) empty_dense_vector!(daf, "gene", "marker", Bool; overwrite = false) do empty_vector
                        @assert false
                    end
                end

                nested_test("true") do
                    @test empty_dense_vector!(daf, "gene", "marker", Bool; overwrite = true) do empty_vector
                        empty_vector .= MARKER_GENES_BY_DEPTH[depth]
                        return 7
                    end == 7
                    @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]; overwrite = true) === nothing

                    nested_test("overwritten") do
                        test_existing_vector(daf, depth + 1)
                        return nothing
                    end
                end
            end
        end

        nested_test("sparse") do
            nested_test("()") do
                return @test_throws dedent("""
                    existing vector: marker
                    for the axis: gene
                    in the daf data: $(daf.name)
                """) empty_sparse_vector!(
                    daf,
                    "gene",
                    "marker",
                    Bool,
                    sum(MARKER_GENES_BY_DEPTH[depth]),
                    Int16,
                ) do empty_nzind, empty_nzval
                    @assert false
                end
            end

            nested_test("overwrite") do
                nested_test("false") do
                    @test_throws dedent("""
                        existing vector: marker
                        for the axis: gene
                        in the daf data: $(daf.name)
                    """) empty_sparse_vector!(
                        daf,
                        "gene",
                        "marker",
                        Bool,
                        sum(MARKER_GENES_BY_DEPTH[depth]),
                        Int16;
                        overwrite = false,
                    ) do empty_nzind, empty_nzval
                        @assert false
                    end
                end

                nested_test("true") do
                    @test empty_sparse_vector!(
                        daf,
                        "gene",
                        "marker",
                        Bool,
                        sum(MARKER_GENES_BY_DEPTH[depth]),
                        Int16;
                        overwrite = true,
                    ) do empty_nzind, empty_nzval
                        sparse = sparse_vector(MARKER_GENES_BY_DEPTH[depth])
                        empty_nzind .= sparse.nzind
                        empty_nzval .= sparse.nzval
                        return 7
                    end == 7
                    @test set_vector!(daf, "cell", "type", CELL_TYPES_BY_DEPTH[depth]; overwrite = true) === nothing
                    test_existing_vector(daf, depth + 1)
                    return nothing
                end
            end
        end
    end
end

function test_missing_matrix_axis(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_missing_matrix_axis(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_matrix") do
        @test_throws dedent("""
            missing axis: cell
            for the rows of the matrix: UMIs
            of the daf data: $(daf.name)
        """) has_matrix(daf, "cell", "gene", "UMIs")

        if daf isa DafWriter
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test_throws dedent("""
                missing axis: gene
                for the columns of the matrix: UMIs
                of the daf data: $(daf.name)
            """) has_matrix(daf, "cell", "gene", "UMIs")
        end
    end

    nested_test("matrices_set") do
        @test_throws dedent("""
            missing axis: cell
            for the rows of: matrices_set
            of the daf data: $(daf.name)
        """) matrices_set(daf, "cell", "gene")

        if daf isa DafWriter
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test_throws dedent("""
                missing axis: gene
                for the columns of: matrices_set
                of the daf data: $(daf.name)
            """) matrices_set(daf, "cell", "gene")
        end
    end

    nested_test("get_matrix") do
        @test_throws dedent("""
            missing axis: cell
            for the rows of the matrix: UMIs
            of the daf data: $(daf.name)
        """) get_matrix(daf, "cell", "gene", "UMIs")

        if daf isa DafWriter
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test_throws dedent("""
                missing axis: gene
                for the columns of the matrix: UMIs
                of the daf data: $(daf.name)
            """) get_matrix(daf, "cell", "gene", "UMIs")
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_matrix") do
        nested_test("()") do
            @test_throws dedent("""
                missing axis: cell
                for the rows of the matrix: UMIs
                of the daf data: $(daf.name)
            """) delete_matrix!(daf, "cell", "gene", "UMIs")
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test_throws dedent("""
                missing axis: gene
                for the columns of the matrix: UMIs
                of the daf data: $(daf.name)
            """) delete_matrix!(daf, "cell", "gene", "UMIs")
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test_throws dedent("""
                    missing axis: cell
                    for the rows of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true)
                @test add_axis!(daf, "cell", CELL_NAMES) === nothing
                @test_throws dedent("""
                    missing axis: gene
                    for the columns of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true)
            end

            nested_test("false") do
                @test_throws dedent("""
                    missing axis: cell
                    for the rows of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = false)
                @test add_axis!(daf, "cell", CELL_NAMES) === nothing
                @test_throws dedent("""
                    missing axis: gene
                    for the columns of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = false)
            end
        end
    end

    nested_test("set_matrix!") do
        nested_test("()") do
            @test_throws dedent("""
                missing axis: cell
                for the rows of the matrix: UMIs
                of the daf data: $(daf.name)
            """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth])
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test_throws dedent("""
                missing axis: gene
                for the columns of the matrix: UMIs
                of the daf data: $(daf.name)
            """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth])
        end

        nested_test("overwrite") do
            nested_test("false") do
                @test_throws dedent("""
                    missing axis: cell
                    for the rows of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = false)
                @test add_axis!(daf, "cell", CELL_NAMES) === nothing
                @test_throws dedent("""
                    missing axis: gene
                    for the columns of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = false)
            end

            nested_test("true") do
                @test_throws dedent("""
                    missing axis: cell
                    for the rows of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = true)
                @test add_axis!(daf, "cell", CELL_NAMES) === nothing
                @test_throws dedent("""
                    missing axis: gene
                    for the columns of the matrix: UMIs
                    of the daf data: $(daf.name)
                """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = true)
            end
        end
    end
end

function test_missing_matrix(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_missing_matrix(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_matrix") do
        @test !has_matrix(daf, "cell", "gene", "UMIs"; relayout = false)
    end

    nested_test("matrices_set") do
        @test isempty(matrices_set(daf, "cell", "gene"; relayout = false))
    end

    nested_test("get_matrix") do
        nested_test("relayout") do
            nested_test("default") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    (and the other way around)
                    in the daf data: $(daf.name)
                """) get_matrix(daf, "cell", "gene", "UMIs")
            end

            nested_test("true") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    (and the other way around)
                    in the daf data: $(daf.name)
                """) get_matrix(daf, "cell", "gene", "UMIs"; relayout = true)
            end

            nested_test("false") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    in the daf data: $(daf.name)
                """) get_matrix(daf, "cell", "gene", "UMIs"; relayout = false)
            end

            if daf isa DafWriter
                nested_test("square") do
                    set_matrix!(daf, "cell", "cell", "outgoing_edges", [0 1 2; 1 0 2; 1 2 0])
                    @test_throws dedent("""
                        can't relayout square matrix: outgoing_edges
                        of the axis: cell
                        due to daf representation limitations
                        in the daf data: $(daf.name)
                    """) relayout_matrix!(daf, "cell", "cell", "outgoing_edges")
                end
            end
        end

        nested_test("default") do
            nested_test("missing") do
                @test get_matrix(daf, "cell", "gene", "UMIs"; default = nothing) === nothing
            end

            nested_test("scalar") do
                @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1.0) ==
                      [1.0 1.0 1.0 1.0; 1.0 1.0 1.0 1.0; 1.0 1.0 1.0 1.0]
                @test get_matrix(daf, "cell", "gene", "UMIs"; default = 0.0) ==
                      [0.0 0.0 0.0 0.0; 0.0 0.0 0.0 0.0; 0.0 0.0 0.0 0.0]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1.0)) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 0.0)) == [CELL_NAMES, GENE_NAMES]
            end

            nested_test("matrix") do
                nested_test("()") do
                    @test get_matrix(
                        daf,
                        "cell",
                        "gene",
                        "UMIs";
                        default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                        relayout = false,
                    ) == [0 1 2 3; 1 2 3 4; 2 3 4 5]
                    @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5])) ==
                          ["cell", "gene"]
                    @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5])) ==
                          [CELL_NAMES, GENE_NAMES]
                end

                nested_test("transpose") do
                    @test_throws "type not in column-major layout: 3 x 4 x Int64 in Rows (Transpose Dense)" get_matrix(
                        daf,
                        "cell",
                        "gene",
                        "UMIs";
                        default = transpose([0 1 2; 1 2 3; 2 3 4; 4 5 6]),
                    )
                end

                nested_test("!rows") do
                    @test_throws dedent("""
                        the length: 2
                        of the rows of the default for the matrix: UMIs
                        is different from the length: 3
                        of the axis: cell
                        in the daf data: $(daf.name)
                    """) get_matrix(daf, "cell", "gene", "UMIs"; default = [1 2 3 4; 2 3 4 5])
                end

                nested_test("!columns") do
                    @test_throws dedent("""
                        the length: 2
                        of the columns of the default for the matrix: UMIs
                        is different from the length: 4
                        of the axis: gene
                        in the daf data: $(daf.name)
                    """) get_matrix(daf, "cell", "gene", "UMIs"; default = [1 2; 2 3; 3 4])
                end
            end

            nested_test("named") do
                nested_test("()") do
                    @test get_matrix(
                        daf,
                        "cell",
                        "gene",
                        "UMIs";
                        default = NamedArray(
                            [0 1 2 3; 1 2 3 4; 2 3 4 5];
                            names = (CELL_NAMES, GENE_NAMES),
                            dimnames = ("cell", "gene"),
                        ),
                    ) == [0 1 2 3; 1 2 3 4; 2 3 4 5]
                    @test dimnames(
                        get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = NamedArray(
                                [0 1 2 3; 1 2 3 4; 2 3 4 5];
                                names = (CELL_NAMES, GENE_NAMES),
                                dimnames = ("cell", "gene"),
                            ),
                        ),
                    ) == ["cell", "gene"]
                    @test names(
                        get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = NamedArray(
                                [0 1 2 3; 1 2 3 4; 2 3 4 5];
                                names = (CELL_NAMES, GENE_NAMES),
                                dimnames = ("cell", "gene"),
                            ),
                        ),
                    ) == [CELL_NAMES, GENE_NAMES]
                end

                nested_test("!rows") do
                    nested_test("name") do
                        @test_throws dedent("""
                            row names of the: default
                            mismatch the entry names of the axis: cell
                            in the daf data: $(daf.name)
                        """) get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = NamedArray(
                                [0 1 2 3; 1 2 3 4; 2 3 4 5];
                                names = (["A", "B", "C"], GENE_NAMES),
                                dimnames = ("cell", "gene"),
                            ),
                        )
                    end

                    nested_test("dim") do
                        @test_throws dedent("""
                            default rows dim name: A
                            is different from the rows axis: cell
                            in the daf data: $(daf.name)
                        """) get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = NamedArray(
                                [0 1 2 3; 1 2 3 4; 2 3 4 5];
                                names = (CELL_NAMES, GENE_NAMES),
                                dimnames = ("A", "gene"),
                            ),
                        )
                    end
                end

                nested_test("!columns") do
                    nested_test("name") do
                        @test_throws dedent("""
                            column names of the: default
                            mismatch the entry names of the axis: gene
                            in the daf data: $(daf.name)
                        """) get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = NamedArray(
                                [0 1 2 3; 1 2 3 4; 2 3 4 5];
                                names = (CELL_NAMES, ["A", "B", "C", "D"]),
                                dimnames = ("cell", "gene"),
                            ),
                        )
                    end

                    nested_test("dim") do
                        @test_throws dedent("""
                            default columns dim name: B
                            is different from the columns axis: gene
                            in the daf data: $(daf.name)
                        """) get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = NamedArray(
                                [0 1 2 3; 1 2 3 4; 2 3 4 5];
                                names = (CELL_NAMES, GENE_NAMES),
                                dimnames = ("cell", "B"),
                            ),
                        )
                    end
                end
            end
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_matrix") do
        nested_test("relayout") do
            nested_test("default") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    (and the other way around)
                    in the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs")
            end

            nested_test("true") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    (and the other way around)
                    in the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs"; relayout = true)
            end

            nested_test("false") do
                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: cell
                    and the columns axis: gene
                    in the daf data: $(daf.name)
                """) delete_matrix!(daf, "cell", "gene", "UMIs"; relayout = false)
            end
        end

        nested_test("must_exist") do
            nested_test("true") do
                nested_test("relayout") do
                    nested_test("default") do
                        @test_throws dedent("""
                            missing matrix: UMIs
                            for the rows axis: cell
                            and the columns axis: gene
                            (and the other way around)
                            in the daf data: $(daf.name)
                        """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true)
                    end

                    nested_test("true") do
                        @test_throws dedent("""
                            missing matrix: UMIs
                            for the rows axis: cell
                            and the columns axis: gene
                            (and the other way around)
                            in the daf data: $(daf.name)
                        """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true, relayout = true)
                    end

                    nested_test("false") do
                        @test_throws dedent("""
                            missing matrix: UMIs
                            for the rows axis: cell
                            and the columns axis: gene
                            in the daf data: $(daf.name)
                        """) delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true, relayout = false)
                    end
                end
            end

            nested_test("false") do
                @test delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = false) === nothing
            end
        end
    end

    nested_test("set_matrix!") do
        nested_test("scalar") do
            @test set_matrix!(daf, "cell", "gene", "UMIs", 1.0; relayout = false) === nothing
            @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = false) ==
                  [1.0 1.0 1.0 1.0; 1.0 1.0 1.0 1.0; 1.0 1.0 1.0 1.0]
            @test set_matrix!(daf, "cell", "gene", "LogUMIs", 0.0; relayout = false, eltype = Int32) === nothing
            @test get_matrix(daf, "cell", "gene", "LogUMIs"; relayout = false) == [0 0 0 0; 0 0 0 0; 0 0 0 0]
            @test eltype(get_matrix(daf, "cell", "gene", "LogUMIs"; relayout = false)) == Int32
        end

        nested_test("matrix") do
            nested_test("relayout") do
                nested_test("dense") do
                    nested_test("exists") do
                        @test set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]) === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("eltype") do
                        @test set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; eltype = UInt32) ===
                              nothing
                        @test eltype(get_matrix(daf, "cell", "gene", "UMIs")) == UInt32
                    end
                end

                nested_test("sparse") do
                    nested_test("exists") do
                        @test set_matrix!(daf, "cell", "gene", "UMIs", sparse_matrix_csc(UMIS_BY_DEPTH[depth])) ===
                              nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("eltype") do
                        @test set_matrix!(
                            daf,
                            "cell",
                            "gene",
                            "UMIs",
                            sparse_matrix_csc(UMIS_BY_DEPTH[depth]);
                            eltype = UInt32,
                        ) === nothing
                        @test eltype(get_matrix(daf, "cell", "gene", "UMIs")) == UInt32
                    end
                end

                nested_test("true") do
                    nested_test("exists") do
                        @test set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; relayout = true) ===
                              nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("eltype") do
                        @test set_matrix!(
                            daf,
                            "cell",
                            "gene",
                            "UMIs",
                            UMIS_BY_DEPTH[depth];
                            relayout = true,
                            eltype = Float32,
                        ) === nothing
                        @test eltype(get_matrix(daf, "cell", "gene", "UMIs")) == Float32
                    end
                end

                nested_test("false") do
                    @test set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; relayout = false) === nothing

                    nested_test("exists") do
                        test_existing_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("relayout") do
                        @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end
                end
            end

            nested_test("transpose") do
                @test_throws "type not in column-major layout: 3 x 4 x Int64 in Rows (Transpose Dense)" set_matrix!(
                    daf,
                    "cell",
                    "gene",
                    "UMIs",
                    transpose([0 1 2; 1 2 3; 2 3 4; 4 5 6]),
                )
            end

            nested_test("!rows") do
                @test_throws dedent("""
                    the length: 2
                    of the rows of the matrix: UMIs
                    is different from the length: 3
                    of the axis: cell
                    in the daf data: $(daf.name)
                """) set_matrix!(daf, "cell", "gene", "UMIs", [1 2 3 4; 2 3 4 5])
            end

            nested_test("!columns") do
                @test_throws dedent("""
                    the length: 2
                    of the columns of the matrix: UMIs
                    is different from the length: 4
                    of the axis: gene
                    in the daf data: $(daf.name)
                """) set_matrix!(daf, "cell", "gene", "UMIs", [1 2; 2 3; 3 4])
            end
        end

        nested_test("named") do
            nested_test("relayout") do
                nested_test("default") do
                    @test set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(UMIS_BY_DEPTH[depth]; names = (CELL_NAMES, GENE_NAMES), dimnames = ("cell", "gene")),
                    ) === nothing
                    test_existing_relayout_matrix(daf, depth + 1)
                    return nothing
                end

                nested_test("true") do
                    @test set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(UMIS_BY_DEPTH[depth]; names = (CELL_NAMES, GENE_NAMES), dimnames = ("cell", "gene"));
                        relayout = true,
                    ) === nothing
                    test_existing_relayout_matrix(daf, depth + 1)
                    return nothing
                end

                nested_test("false") do
                    previous_version_counter = matrix_version_counter(daf, "gene", "cell", "UMIS")
                    @test set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(UMIS_BY_DEPTH[depth]; names = (CELL_NAMES, GENE_NAMES), dimnames = ("cell", "gene"));
                        relayout = false,
                    ) === nothing
                    @test matrix_version_counter(daf, "cell", "gene", "UMIs") == previous_version_counter + 1

                    nested_test("exists") do
                        test_existing_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("relayout") do
                        @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end
                end
            end

            nested_test("!rows") do
                nested_test("name") do
                    @test_throws dedent("""
                        row names of the: matrix
                        mismatch the entry names of the axis: cell
                        in the daf data: $(daf.name)
                    """) set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(
                            UMIS_BY_DEPTH[depth];
                            names = (["A", "B", "C"], GENE_NAMES),
                            dimnames = ("cell", "gene"),
                        ),
                    )
                end

                nested_test("dim") do
                    @test_throws dedent("""
                        matrix rows dim name: A
                        is different from the rows axis: cell
                        in the daf data: $(daf.name)
                    """) set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(UMIS_BY_DEPTH[depth]; names = (CELL_NAMES, GENE_NAMES), dimnames = ("A", "gene")),
                    )
                end
            end

            nested_test("!columns") do
                nested_test("name") do
                    @test_throws dedent("""
                        column names of the: matrix
                        mismatch the entry names of the axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(
                            UMIS_BY_DEPTH[depth];
                            names = (CELL_NAMES, ["A", "B", "C", "D"]),
                            dimnames = ("cell", "gene"),
                        ),
                    )
                end

                nested_test("dim") do
                    @test_throws dedent("""
                        matrix columns dim name: B
                        is different from the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        NamedArray(UMIS_BY_DEPTH[depth]; names = (CELL_NAMES, GENE_NAMES), dimnames = ("cell", "B")),
                    )
                end
            end
        end
    end

    nested_test("empty_matrix!") do
        nested_test("dense") do
            previous_version_counter = matrix_version_counter(daf, "cell", "gene", "UMIs")
            @test empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16) do empty_matrix
                empty_matrix .= UMIS_BY_DEPTH[depth]
                return 7
            end == 7
            @test matrix_version_counter(daf, "gene", "cell", "UMIs") == previous_version_counter + 1

            nested_test("exists") do
                test_existing_matrix(daf, depth + 1)
                return nothing
            end

            nested_test("relayout") do
                @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                test_existing_relayout_matrix(daf, depth + 1)
                return nothing
            end
        end

        nested_test("sparse") do
            previous_version_counter = matrix_version_counter(daf, "cell", "gene", "UMIs")
            @test empty_sparse_matrix!(
                daf,
                "cell",
                "gene",
                "UMIs",
                Int16,
                sum(UMIS_BY_DEPTH[depth] .> 0),
                Int16,
            ) do empty_colptr, empty_rowval, empty_nzval
                sparse = sparse_matrix_csc(UMIS_BY_DEPTH[depth])
                empty_colptr .= sparse.colptr
                empty_rowval .= sparse.rowval
                empty_nzval .= sparse.nzval
                return 7
            end == 7
            @test matrix_version_counter(daf, "gene", "cell", "UMIs") == previous_version_counter + 1

            nested_test("exists") do
                test_existing_matrix(daf, depth + 1)
                return nothing
            end

            nested_test("relayout") do
                @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                test_existing_relayout_matrix(daf, depth + 1)
                return nothing
            end
        end
    end
end

function test_existing_matrix(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_existing_matrix(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_matrix") do
        nested_test("relayout") do
            nested_test("default") do
                @test has_matrix(daf, "cell", "gene", "UMIs")
                @test has_matrix(daf, "gene", "cell", "UMIs")
            end

            nested_test("true") do
                @test has_matrix(daf, "cell", "gene", "UMIs"; relayout = true)
                @test has_matrix(daf, "gene", "cell", "UMIs"; relayout = true)
            end

            nested_test("false") do
                @test has_matrix(daf, "cell", "gene", "UMIs"; relayout = false)
                @test !has_matrix(daf, "gene", "cell", "UMIs"; relayout = false)
            end
        end
    end

    nested_test("matrices_set") do
        nested_test("relayout") do
            nested_test("default") do
                @test matrices_set(daf, "cell", "gene") == Set(["UMIs"])
                @test matrices_set(daf, "gene", "cell") == Set(["UMIs"])
            end

            nested_test("true") do
                @test matrices_set(daf, "cell", "gene"; relayout = true) == Set(["UMIs"])
                @test matrices_set(daf, "gene", "cell"; relayout = true) == Set(["UMIs"])
            end

            nested_test("false") do
                @test matrices_set(daf, "cell", "gene"; relayout = false) == Set(["UMIs"])
                @test isempty(matrices_set(daf, "gene", "cell"; relayout = false))
            end
        end
    end

    nested_test("get_matrix") do
        nested_test("relayout") do
            nested_test("default") do
                @test get_matrix(daf, "cell", "gene", "UMIs") == UMIS_BY_DEPTH[depth - 1]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs")) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs")) == [CELL_NAMES, GENE_NAMES]

                @test get_matrix(daf, "gene", "cell", "UMIs") == transpose(UMIS_BY_DEPTH[depth - 1])
                @test dimnames(get_matrix(daf, "gene", "cell", "UMIs")) == ["gene", "cell"]
                @test names(get_matrix(daf, "gene", "cell", "UMIs")) == [GENE_NAMES, CELL_NAMES]

                if !(daf isa DafWriter)
                    daf = daf.daf
                end

                nested_test("!axes") do
                    @test delete_axis!(daf, "cell") === nothing
                    @test delete_axis!(daf, "gene") === nothing
                    test_missing_matrix_axis(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("true") do
                @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = true) == UMIS_BY_DEPTH[depth - 1]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; relayout = true)) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs"; relayout = true)) == [CELL_NAMES, GENE_NAMES]

                @test get_matrix(daf, "gene", "cell", "UMIs"; relayout = true) == transpose(UMIS_BY_DEPTH[depth - 1])
                @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; relayout = true)) == ["gene", "cell"]
                @test names(get_matrix(daf, "gene", "cell", "UMIs"; relayout = true)) == [GENE_NAMES, CELL_NAMES]

                if !(daf isa DafWriter)
                    daf = daf.daf
                end

                nested_test("!axes") do
                    @test delete_axis!(daf, "cell") === nothing
                    @test delete_axis!(daf, "gene") === nothing
                    test_missing_matrix_axis(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = false) == UMIS_BY_DEPTH[depth - 1]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; relayout = false)) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs"; relayout = false)) == [CELL_NAMES, GENE_NAMES]

                @test_throws dedent("""
                    missing matrix: UMIs
                    for the rows axis: gene
                    and the columns axis: cell
                    in the daf data: $(daf.name)
                """) get_matrix(daf, "gene", "cell", "UMIs"; relayout = false)
            end
        end

        nested_test("default") do
            nested_test("scalar") do
                nested_test("relayout") do
                    nested_test("default") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1) == UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1)) == ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 1)) == [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = 1) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; default = 1)) == ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = 1)) == [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("true") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = true) ==
                              UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = true)) ==
                              ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = true)) ==
                              [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true)) ==
                              ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true)) ==
                              [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("false") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = false) ==
                              UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = false)) ==
                              ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = false)) ==
                              [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = false) ==
                              [1 1 1; 1 1 1; 1 1 1; 1 1 1]
                        @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true)) ==
                              ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true)) ==
                              [GENE_NAMES, CELL_NAMES]
                    end
                end
            end

            nested_test("matrix") do
                nested_test("relayout") do
                    nested_test("default") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5]) ==
                              UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(
                            get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5]),
                        ) == ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5])) ==
                              [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = [0 1 2; 1 2 3; 2 3 4; 4 5 6]) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(
                            get_matrix(daf, "gene", "cell", "UMIs"; default = [0 1 2; 1 2 3; 2 3 4; 4 5 6]),
                        ) == ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = [0 1 2; 1 2 3; 2 3 4; 4 5 6])) ==
                              [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("true") do
                        @test get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                            relayout = true,
                        ) == UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = true,
                            ),
                        ) == ["cell", "gene"]
                        @test names(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = true,
                            ),
                        ) == [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(
                            daf,
                            "gene",
                            "cell",
                            "UMIs";
                            default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                            relayout = true,
                        ) == transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = true,
                            ),
                        ) == ["gene", "cell"]
                        @test names(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = true,
                            ),
                        ) == [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("false") do
                        @test get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                            relayout = false,
                        ) == UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = false,
                            ),
                        ) == ["cell", "gene"]
                        @test names(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = false,
                            ),
                        ) == [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(
                            daf,
                            "gene",
                            "cell",
                            "UMIs";
                            default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                            relayout = false,
                        ) == [0 1 2; 1 2 3; 2 3 4; 4 5 6]
                        @test dimnames(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = false,
                            ),
                        ) == ["gene", "cell"]
                        @test names(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = false,
                            ),
                        ) == [GENE_NAMES, CELL_NAMES]
                    end
                end

                nested_test("!rows") do
                    @test_throws dedent("""
                        the length: 2
                        of the rows of the default for the matrix: UMIs
                        is different from the length: 3
                        of the axis: cell
                        in the daf data: $(daf.name)
                    """) get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 4 5 6 7])
                end

                nested_test("!columns") do
                    @test_throws dedent("""
                        the length: 2
                        of the columns of the default for the matrix: UMIs
                        is different from the length: 4
                        of the axis: gene
                        in the daf data: $(daf.name)
                    """) get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1; 2 3; 4 5])
                end
            end
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_matrix!") do
        nested_test("()") do
            @test delete_matrix!(daf, "cell", "gene", "UMIs"; relayout = false) === nothing
            nested_test("deleted") do
                test_missing_matrix(daf, depth + 1)
                return nothing
            end
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true, relayout = false) === nothing
                nested_test("deleted") do
                    test_missing_matrix(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = false, relayout = false) === nothing
                nested_test("deleted") do
                    test_missing_matrix(daf, depth + 1)
                    return nothing
                end
            end
        end
    end

    nested_test("delete_axis!") do
        @test delete_axis!(daf, "gene") === nothing
        @test delete_axis!(daf, "cell") === nothing
        test_missing_matrix_axis(daf, depth + 1)
        return nothing
    end

    nested_test("set_matrix!") do
        nested_test("scalar") do
            nested_test("overwrite") do
                nested_test("default") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", 1)
                end

                nested_test("false") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", 1; overwrite = false)
                end

                nested_test("true") do
                    @test set_matrix!(daf, "cell", "gene", "UMIs", 1; overwrite = true, relayout = false) === nothing
                    @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = false) == [1 1 1 1; 1 1 1 1; 1 1 1 1]
                end
            end
        end

        nested_test("matrix") do
            nested_test("overwrite") do
                nested_test("default") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth])
                end

                nested_test("false") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = false)
                end

                nested_test("true") do
                    nested_test("relayout") do
                        nested_test("default") do
                            @test set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = true) ==
                                  nothing
                            nested_test("overwritten") do
                                test_existing_relayout_matrix(daf, depth + 1)
                                return nothing
                            end
                        end

                        nested_test("true") do
                            @test set_matrix!(
                                daf,
                                "cell",
                                "gene",
                                "UMIs",
                                UMIS_BY_DEPTH[depth];
                                overwrite = true,
                                relayout = true,
                            ) === nothing
                            nested_test("overwritten") do
                                test_existing_relayout_matrix(daf, depth + 1)
                                return nothing
                            end
                        end

                        nested_test("false") do
                            @test set_matrix!(
                                daf,
                                "cell",
                                "gene",
                                "UMIs",
                                UMIS_BY_DEPTH[depth];
                                overwrite = true,
                                relayout = false,
                            ) === nothing
                            nested_test("overwritten") do
                                test_existing_matrix(daf, depth + 1)
                                return nothing
                            end
                        end
                    end
                end
            end
        end
    end

    nested_test("empty_matrix!") do
        nested_test("dense") do
            nested_test("overwrite") do
                nested_test("default") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16) do empty_matrix
                        @assert false
                    end
                end

                nested_test("false") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16; overwrite = false) do empty_matrix
                        @assert false
                    end
                end

                nested_test("true") do
                    @test empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16; overwrite = true) do empty_matrix
                        empty_matrix .= UMIS_BY_DEPTH[depth]
                        return 7
                    end == 7

                    nested_test("overwritten") do
                        test_existing_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("relayout") do
                        @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end
                end
            end
        end

        nested_test("sparse") do
            nested_test("overwrite") do
                nested_test("default") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_sparse_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        Int16,
                        sum(UMIS_BY_DEPTH[depth] .> 0),
                        Int16,
                    ) do empty_colptr, empty_rowval, empty_nzval
                        @assert false
                    end
                end

                nested_test("false") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_sparse_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        Int16,
                        sum(UMIS_BY_DEPTH[depth] .> 0),
                        Int16;
                        overwrite = false,
                    ) do empty_colptr, empty_rowval, empty_nzval
                        @assert false
                    end
                end

                nested_test("true") do
                    @test empty_sparse_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        Int16,
                        sum(UMIS_BY_DEPTH[depth] .> 0),
                        Int16;
                        overwrite = true,
                    ) do empty_colptr, empty_rowval, empty_nzval
                        sparse = sparse_matrix_csc(UMIS_BY_DEPTH[depth])
                        empty_colptr .= sparse.colptr
                        empty_rowval .= sparse.rowval
                        empty_nzval .= sparse.nzval
                        return 7
                    end == 7

                    nested_test("overwritten") do
                        test_existing_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("relayout") do
                        @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end
                end
            end
        end
    end
end

function test_existing_relayout_matrix(daf::DafReader, depth::Int)::Nothing
    if depth > 2
        return nothing
    end

    if daf isa DafWriter
        nested_test("read_only") do
            test_existing_relayout_matrix(read_only(daf), depth)
            return nothing
        end
    end

    nested_test("has_matrix") do
        nested_test("relayout") do
            nested_test("default") do
                @test has_matrix(daf, "cell", "gene", "UMIs")
                @test has_matrix(daf, "gene", "cell", "UMIs")
            end

            nested_test("true") do
                @test has_matrix(daf, "cell", "gene", "UMIs"; relayout = true)
                @test has_matrix(daf, "gene", "cell", "UMIs"; relayout = true)
            end

            nested_test("false") do
                @test has_matrix(daf, "cell", "gene", "UMIs"; relayout = false)
                @test has_matrix(daf, "gene", "cell", "UMIs"; relayout = false)
            end
        end
    end

    nested_test("matrices_set") do
        nested_test("relayout") do
            nested_test("default") do
                @test matrices_set(daf, "cell", "gene") == Set(["UMIs"])
                @test matrices_set(daf, "gene", "cell") == Set(["UMIs"])
            end

            nested_test("true") do
                @test matrices_set(daf, "cell", "gene"; relayout = true) == Set(["UMIs"])
                @test matrices_set(daf, "gene", "cell"; relayout = true) == Set(["UMIs"])
            end

            nested_test("false") do
                @test matrices_set(daf, "cell", "gene"; relayout = false) == Set(["UMIs"])
                @test matrices_set(daf, "gene", "cell"; relayout = false) == Set(["UMIs"])
            end
        end
    end

    nested_test("get_matrix") do
        nested_test("relayout") do
            nested_test("default") do
                @test get_matrix(daf, "cell", "gene", "UMIs") == UMIS_BY_DEPTH[depth - 1]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs")) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs")) == [CELL_NAMES, GENE_NAMES]

                @test get_matrix(daf, "gene", "cell", "UMIs") == transpose(UMIS_BY_DEPTH[depth - 1])
                @test dimnames(get_matrix(daf, "gene", "cell", "UMIs")) == ["gene", "cell"]
                @test names(get_matrix(daf, "gene", "cell", "UMIs")) == [GENE_NAMES, CELL_NAMES]

                if !(daf isa DafWriter)
                    daf = daf.daf
                end

                nested_test("!axes") do
                    @test delete_axis!(daf, "cell") === nothing
                    @test delete_axis!(daf, "gene") === nothing
                    test_missing_matrix_axis(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("true") do
                @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = true) == UMIS_BY_DEPTH[depth - 1]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; relayout = true)) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs"; relayout = true)) == [CELL_NAMES, GENE_NAMES]

                @test get_matrix(daf, "gene", "cell", "UMIs"; relayout = true) == transpose(UMIS_BY_DEPTH[depth - 1])
                @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; relayout = true)) == ["gene", "cell"]
                @test names(get_matrix(daf, "gene", "cell", "UMIs"; relayout = true)) == [GENE_NAMES, CELL_NAMES]
            end

            nested_test("false") do
                @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = false) == UMIS_BY_DEPTH[depth - 1]
                @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; relayout = false)) == ["cell", "gene"]
                @test names(get_matrix(daf, "cell", "gene", "UMIs"; relayout = false)) == [CELL_NAMES, GENE_NAMES]

                @test get_matrix(daf, "gene", "cell", "UMIs"; relayout = false) == transpose(UMIS_BY_DEPTH[depth - 1])
                @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; relayout = false)) == ["gene", "cell"]
                @test names(get_matrix(daf, "gene", "cell", "UMIs"; relayout = false)) == [GENE_NAMES, CELL_NAMES]
            end
        end

        nested_test("default") do
            nested_test("scalar") do
                nested_test("relayout") do
                    nested_test("default") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1) == UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1)) == ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 1)) == [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = 1) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; default = 1)) == ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = 1)) == [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("true") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = true) ==
                              UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = true)) ==
                              ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = true)) ==
                              [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true)) ==
                              ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = true)) ==
                              [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("false") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = false) ==
                              UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = false)) ==
                              ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = 1, relayout = false)) ==
                              [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = false) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = false)) ==
                              ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = 1, relayout = false)) ==
                              [GENE_NAMES, CELL_NAMES]
                    end
                end
            end

            nested_test("matrix") do
                nested_test("relayout") do
                    nested_test("default") do
                        @test get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5]) ==
                              UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(
                            get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5]),
                        ) == ["cell", "gene"]
                        @test names(get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 1 2 3 4; 2 3 4 5])) ==
                              [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(daf, "gene", "cell", "UMIs"; default = [0 1 2; 1 2 3; 2 3 4; 4 5 6]) ==
                              transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(
                            get_matrix(daf, "gene", "cell", "UMIs"; default = [0 1 2; 1 2 3; 2 3 4; 4 5 6]),
                        ) == ["gene", "cell"]
                        @test names(get_matrix(daf, "gene", "cell", "UMIs"; default = [0 1 2; 1 2 3; 2 3 4; 4 5 6])) ==
                              [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("true") do
                        @test get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                            relayout = true,
                        ) == UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = true,
                            ),
                        ) == ["cell", "gene"]
                        @test names(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = true,
                            ),
                        ) == [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(
                            daf,
                            "gene",
                            "cell",
                            "UMIs";
                            default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                            relayout = true,
                        ) == transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = true,
                            ),
                        ) == ["gene", "cell"]
                        @test names(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = true,
                            ),
                        ) == [GENE_NAMES, CELL_NAMES]
                    end

                    nested_test("false") do
                        @test get_matrix(
                            daf,
                            "cell",
                            "gene",
                            "UMIs";
                            default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                            relayout = false,
                        ) == UMIS_BY_DEPTH[depth - 1]
                        @test dimnames(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = false,
                            ),
                        ) == ["cell", "gene"]
                        @test names(
                            get_matrix(
                                daf,
                                "cell",
                                "gene",
                                "UMIs";
                                default = [0 1 2 3; 1 2 3 4; 2 3 4 5],
                                relayout = false,
                            ),
                        ) == [CELL_NAMES, GENE_NAMES]

                        @test get_matrix(
                            daf,
                            "gene",
                            "cell",
                            "UMIs";
                            default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                            relayout = false,
                        ) == transpose(UMIS_BY_DEPTH[depth - 1])
                        @test dimnames(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = false,
                            ),
                        ) == ["gene", "cell"]
                        @test names(
                            get_matrix(
                                daf,
                                "gene",
                                "cell",
                                "UMIs";
                                default = [0 1 2; 1 2 3; 2 3 4; 4 5 6],
                                relayout = false,
                            ),
                        ) == [GENE_NAMES, CELL_NAMES]
                    end
                end

                nested_test("!rows") do
                    @test_throws dedent("""
                        the length: 2
                        of the rows of the default for the matrix: UMIs
                        is different from the length: 3
                        of the axis: cell
                        in the daf data: $(daf.name)
                    """) get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1 2 3; 4 5 6 7])
                end

                nested_test("!columns") do
                    @test_throws dedent("""
                        the length: 2
                        of the columns of the default for the matrix: UMIs
                        is different from the length: 4
                        of the axis: gene
                        in the daf data: $(daf.name)
                    """) get_matrix(daf, "cell", "gene", "UMIs"; default = [0 1; 2 3; 4 5])
                end
            end
        end
    end

    if !(daf isa DafWriter)
        return nothing
    end

    nested_test("delete_matrix!") do
        nested_test("relayout") do
            nested_test("default") do
                @test delete_matrix!(daf, "gene", "cell", "UMIs") === nothing
                nested_test("deleted") do
                    test_missing_matrix(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("true") do
                @test delete_matrix!(daf, "gene", "cell", "UMIs"; relayout = true) === nothing
                nested_test("deleted") do
                    test_missing_matrix(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test delete_matrix!(daf, "gene", "cell", "UMIs"; relayout = false) === nothing
                nested_test("deleted") do
                    test_existing_matrix(daf, depth)
                    return nothing
                end
            end
        end

        nested_test("must_exist") do
            nested_test("true") do
                @test delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = true) === nothing
                nested_test("deleted") do
                    test_missing_matrix(daf, depth + 1)
                    return nothing
                end
            end

            nested_test("false") do
                @test delete_matrix!(daf, "cell", "gene", "UMIs"; must_exist = false) === nothing
                nested_test("deleted") do
                    test_missing_matrix(daf, depth + 1)
                    return nothing
                end
            end
        end
    end

    nested_test("delete_axis!") do
        @test delete_axis!(daf, "gene") === nothing
        @test delete_axis!(daf, "cell") === nothing
        test_missing_matrix_axis(daf, depth + 1)
        return nothing
    end

    nested_test("set_matrix!") do
        nested_test("scalar") do
            nested_test("overwrite") do
                nested_test("default") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", 1)
                end

                nested_test("false") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", 1; overwrite = false)
                end

                nested_test("true") do
                    @test set_matrix!(daf, "cell", "gene", "UMIs", 1; overwrite = true, relayout = false) === nothing
                    @test get_matrix(daf, "cell", "gene", "UMIs"; relayout = false) == [1 1 1 1; 1 1 1 1; 1 1 1 1]
                end
            end
        end

        nested_test("matrix") do
            nested_test("overwrite") do
                nested_test("default") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth])
                end

                nested_test("false") do
                    @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = false)
                end

                nested_test("true") do
                    nested_test("relayout") do
                        nested_test("default") do
                            @test set_matrix!(daf, "cell", "gene", "UMIs", UMIS_BY_DEPTH[depth]; overwrite = true) ==
                                  nothing
                            nested_test("overwritten") do
                                test_existing_relayout_matrix(daf, depth + 1)
                                return nothing
                            end
                        end

                        nested_test("true") do
                            @test set_matrix!(
                                daf,
                                "cell",
                                "gene",
                                "UMIs",
                                UMIS_BY_DEPTH[depth];
                                overwrite = true,
                                relayout = true,
                            ) === nothing
                            nested_test("overwritten") do
                                test_existing_relayout_matrix(daf, depth + 1)
                                return nothing
                            end

                            nested_test("relayout") do
                                nested_test("overwrite") do
                                    nested_test("default") do
                                        @test_throws dedent("""
                                            existing matrix: UMIs
                                            for the rows axis: gene
                                            and the columns axis: cell
                                            in the daf data: $(daf.name)
                                        """) relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                                    end

                                    nested_test("false") do
                                        @test_throws dedent("""
                                            existing matrix: UMIs
                                            for the rows axis: gene
                                            and the columns axis: cell
                                            in the daf data: $(daf.name)
                                        """) relayout_matrix!(daf, "cell", "gene", "UMIs"; overwrite = false) ===
                                             nothing
                                    end

                                    nested_test("true") do
                                        @test relayout_matrix!(daf, "cell", "gene", "UMIs"; overwrite = true) ===
                                              nothing
                                        test_existing_relayout_matrix(daf, depth + 1)
                                        return nothing
                                    end
                                end
                            end
                        end

                        nested_test("false") do
                            @test delete_matrix!(daf, "cell", "gene", "UMIs"; relayout = true) === nothing

                            @test set_matrix!(
                                daf,
                                "cell",
                                "gene",
                                "UMIs",
                                UMIS_BY_DEPTH[depth];
                                overwrite = true,
                                relayout = false,
                            ) === nothing

                            nested_test("delete") do
                                @test_throws dedent("""
                                    missing matrix: UMIs
                                    for the rows axis: gene
                                    and the columns axis: cell
                                    in the daf data: $(daf.name)
                                """) delete_matrix!(daf, "gene", "cell", "UMIs"; relayout = false) === nothing
                                test_existing_matrix(daf, depth + 1)
                                return nothing
                            end

                            nested_test("relayout") do
                                @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                                test_existing_relayout_matrix(daf, depth + 1)
                                return nothing
                            end
                        end
                    end
                end
            end
        end
    end

    nested_test("empty_matrix!") do
        nested_test("dense") do
            nested_test("overwrite") do
                nested_test("default") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16) do empty_matrix
                        @assert false
                    end
                end

                nested_test("false") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16; overwrite = false) do empty_matrix
                        @assert false
                    end
                end

                nested_test("true") do
                    @test empty_dense_matrix!(daf, "cell", "gene", "UMIs", Int16; overwrite = true) do empty_matrix
                        empty_matrix .= UMIS_BY_DEPTH[depth]
                        return 7
                    end == 7
                    @test delete_matrix!(daf, "gene", "cell", "UMIs"; relayout = false) === nothing

                    nested_test("overwritten") do
                        test_existing_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("relayout") do
                        @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end
                end
            end
        end

        nested_test("sparse") do
            nested_test("overwrite") do
                nested_test("default") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_sparse_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        Int16,
                        sum(UMIS_BY_DEPTH[depth] .> 0),
                        Int16,
                    ) do empty_colptr, empty_rowval, empty_nzval
                        @assert false
                    end
                end

                nested_test("false") do
                    return @test_throws dedent("""
                        existing matrix: UMIs
                        for the rows axis: cell
                        and the columns axis: gene
                        in the daf data: $(daf.name)
                    """) empty_sparse_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        Int16,
                        sum(UMIS_BY_DEPTH[depth] .> 0),
                        Int16;
                        overwrite = false,
                    ) do empty_colptr, empty_rowval, empty_nzval
                        @assert false
                    end
                end

                nested_test("true") do
                    @test empty_sparse_matrix!(
                        daf,
                        "cell",
                        "gene",
                        "UMIs",
                        Int16,
                        sum(UMIS_BY_DEPTH[depth] .> 0),
                        Int16;
                        overwrite = true,
                    ) do empty_colptr, empty_rowval, empty_nzval
                        sparse = sparse_matrix_csc(UMIS_BY_DEPTH[depth])
                        empty_colptr .= sparse.colptr
                        empty_rowval .= sparse.rowval
                        empty_nzval .= sparse.nzval
                        return 7
                    end == 7
                    @test delete_matrix!(daf, "gene", "cell", "UMIs"; relayout = false) === nothing

                    nested_test("overwritten") do
                        test_existing_matrix(daf, depth + 1)
                        return nothing
                    end

                    nested_test("relayout") do
                        @test relayout_matrix!(daf, "cell", "gene", "UMIs") === nothing
                        test_existing_relayout_matrix(daf, depth + 1)
                        return nothing
                    end
                end
            end
        end
    end
end

function test_format(daf::DafWriter)
    nested_test("scalar") do
        nested_test("missing") do
            test_missing_scalar(daf, 1)
            return nothing
        end
    end

    nested_test("axis") do
        nested_test("missing") do
            test_missing_axis(daf, 1)
            return nothing
        end
    end

    nested_test("vector") do
        nested_test("!axis") do
            test_missing_vector_axis(daf, 1)
            return nothing
        end

        nested_test("axis") do
            @test add_axis!(daf, "gene", GENE_NAMES) === nothing
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            test_missing_vector(daf, 1)
            return nothing
        end
    end

    nested_test("matrix") do
        nested_test("!axes") do
            test_missing_matrix_axis(daf, 1)
            return nothing
        end

        nested_test("axes") do
            @test add_axis!(daf, "cell", CELL_NAMES) === nothing
            @test add_axis!(daf, "gene", GENE_NAMES) === nothing
            test_missing_matrix(daf, 1)
            return nothing
        end
    end
end

nested_test("data") do
    nested_test("memory") do
        daf = MemoryDaf(; name = "memory!")
        @test daf.name == "memory!"
        @test string(daf) == "MemoryDaf memory!"
        @test string(read_only(daf)) == "ReadOnly MemoryDaf memory!.read_only"
        @test string(read_only(daf; name = "read-only memory!")) == "ReadOnly MemoryDaf read-only memory!"
        @test description(daf) == dedent("""
            name: memory!
            type: MemoryDaf
        """) * "\n"
        test_format(daf)
        return nothing
    end

    nested_test("contract") do
        daf = MemoryDaf(; name = "memory!")
        contract = Contract(;
            axes = ["cell" => (OptionalOutput, "cell"), "gene" => (OptionalOutput, "gene")],
            data = [
                "depth" => (OptionalOutput, StorageScalar, "depth"),
                "version" => (OptionalOutput, StorageScalar, "version"),
                ("gene", "marker") => (OptionalOutput, Bool, "is marker"),
                ("gene", "noisy") => (OptionalOutput, Bool, "is noisy"),
                ("cell", "type") => (OptionalOutput, AbstractString, "type"),
                ("cell", "gene", "UMIs") => (OptionalOutput, StorageReal, "UMIs"),
                ("cell", "gene", "LogUMIs") => (OptionalOutput, StorageReal, "LogUMIs"),
                ("cell", "cell", "outgoing_edges") => (OptionalOutput, StorageReal, "UMIs"),
            ],
        )
        contract_daf = contractor("computation", contract, daf; overwrite = true)
        @test contract_daf.name == "memory!.for.computation"
        @test string(contract_daf) == "Contract MemoryDaf memory!.for.computation"
        test_format(contract_daf)
        return nothing
    end

    nested_test("h5df") do
        nested_test("invalid") do
            mktempdir() do path
                h5open(path * "/test.h5df", "w") do h5file
                    @test_throws "invalid mode: a" H5df(h5file, "a")
                    println("Ignore the following warning:")
                    @test_throws "not a daf data set: HDF5.File: (read-write) $(path)/test.h5df" H5df(h5file)
                    println("Ignore the following warning:")
                    @test_logs (:warn, dedent("""
                        unsafe HDF5 file alignment for Daf: (1, 1)
                        the safe HDF5 file alignment is: (1, 8)
                        note that unaligned data is inefficient,
                        and will break the empty_* functions;
                        to force the alignment, create the file using:
                        h5open(...;fapl=HDF5.FileAccessProperties(;alignment=(1,8))
                    """)) H5df(h5file, "w+"; name = "h5df!")
                    delete_object(h5file, "daf")
                    h5file["daf"] = [UInt(2), UInt(0)]
                    @test_throws dedent("""
                        incompatible format version: 2.0
                        for the daf data: HDF5.File: (read-write) $(path)/test.h5df
                        the code supports version: 1.0
                    """) H5df(h5file; name = "version!")
                end
            end
        end

        nested_test("root") do
            mktempdir() do path
                daf = H5df("$(path)/test.h5df", "w+"; name = "h5df!")
                @test daf.name == "h5df!"
                @test string(daf) == "H5df h5df!"
                @test string(read_only(daf)) == "ReadOnly H5df h5df!.read_only"
                @test string(read_only(daf; name = "renamed!")) == "ReadOnly H5df renamed!"
                @test description(daf) == dedent("""
                    name: h5df!
                    type: H5df
                    root: HDF5.File: (read-write) $(path)/test.h5df
                    mode: w+
                """) * "\n"
                test_format(daf)
                daf = H5df(path * "/test.h5df", "r+")
                @test daf.name == "$(path)/test.h5df"
                @test string(daf) == "H5df $(path)/test.h5df"
                return nothing
            end
        end

        nested_test("nested") do
            mktempdir() do path
                h5open(path * "/test.h5df", "w"; fapl = HDF5.FileAccessProperties(; alignment = (1, 8))) do h5file
                    HDF5.create_group(h5file, "root")
                    daf = H5df(h5file["root"], "w+")
                    @test string(daf) == "H5df $(path)/test.h5df:/root"
                    @test string(read_only(daf)) == "ReadOnly H5df $(path)/test.h5df:/root.read_only"
                    @test string(read_only(daf; name = "renamed!")) == "ReadOnly H5df renamed!"
                    @test description(daf) == dedent("""
                        name: $(path)/test.h5df:/root
                        type: H5df
                        root: HDF5.Group: /root (file: $(path)/test.h5df)
                        mode: w+
                    """) * "\n"
                    test_format(daf)

                    attributes(h5file["root"])["will_be_deleted"] = 1
                    @test length(attributes(h5file["root"])) == 1

                    h5file["root/scalars/name"] = "h5df!"
                    daf = H5df(h5file["root"], "r")
                    @test daf.name == "h5df!.read_only"

                    daf = H5df(h5file["root"], "w"; name = "h5df!")
                    @test daf.name == "h5df!"
                    @test string(daf) == "H5df h5df!"
                    @test length(attributes(h5file["root"])) == 0
                    return nothing
                end
            end
        end
    end

    nested_test("files") do
        nested_test("invalid") do
            mktempdir() do path
                @test_throws "invalid mode: a" FilesDaf(path, "a")
                write("$(path)/file", "")
                @test_throws "not a directory: $(path)/file" FilesDaf("$(path)/file")
                @test_throws "not a daf directory: $(path)" FilesDaf(path)
                open("$(path)/daf.json", "w") do file
                    return println(file, "{\"version\":[2,0]}")
                end
                @test_throws dedent("""
                    incompatible format version: 2.0
                    for the daf directory: $(path)
                    the code supports version: 1.0
                """) FilesDaf(path; name = "version!")
            end
        end

        nested_test("root") do
            mktempdir() do path
                path = path * "/test"
                daf = FilesDaf(path, "w+"; name = "files!")
                @test daf.name == "files!"
                @test string(daf) == "FilesDaf files!"
                @test string(read_only(daf)) == "ReadOnly FilesDaf files!.read_only"
                @test string(read_only(daf; name = "renamed!")) == "ReadOnly FilesDaf renamed!"
                @test description(daf) == dedent("""
                    name: files!
                    type: FilesDaf
                    path: $(path)
                    mode: w+
                """) * "\n"
                test_format(daf)
                mkdir(path * "/deleted")
                daf = FilesDaf(path, "r")
                @test daf.name == "$(path).read_only"
                write(path * "/scalars/name.json", "{\"type\":\"String\",\"value\":\"files!\"}\n")
                daf = FilesDaf(path, "r")
                @test isdir(path * "/deleted")
                @test string(daf) == "ReadOnly FilesDaf files!.read_only"
                daf = FilesDaf(path, "w"; name = "empty!")
                @test string(daf) == "FilesDaf empty!"
                @test !ispath(path * "/deleted")
                return nothing
            end
        end
    end
end
