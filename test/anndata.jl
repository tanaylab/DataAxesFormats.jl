function test_adata(adata::AnnData)::Nothing
    @test adata.obs_names == ["A", "B"]
    @test adata.var_names == ["X", "Y", "Z"]
    @test adata.uns["obs_is"] == "cell"
    @test adata.uns["var_is"] == "gene"
    @test adata.uns["X_is"] == "UMIs"
    @test adata.X == [0 1 2; 3 4 5]
    @test adata.obs[!, "age"] == [0, 1]
    @test adata.layers["LogUMIs"] == log2.([0 1 2; 3 4 5] .+ 1)
    @test adata.obsp["edges"] == transpose([0 1; 2 3])
    return nothing
end

function test_daf(daf::DafReader)::Nothing
    @test axis_vector(daf, "cell") == ["A", "B"]
    @test axis_vector(daf, "gene") == ["X", "Y", "Z"]
    @test get_scalar(daf, "obs_is") == "cell"
    @test get_scalar(daf, "var_is") == "gene"
    @test get_scalar(daf, "X_is") == "UMIs"
    @test get_vector(daf, "cell", "age") == [0, 1]
    @test get_matrix(daf, "cell", "gene", "UMIs") == [0 1 2; 3 4 5]
    @test get_matrix(daf, "cell", "gene", "LogUMIs") == log2.([0 1 2; 3 4 5] .+ 1)
    @test get_matrix(daf, "cell", "cell", "edges") == [0 1; 2 3]
    return nothing
end

nested_test("anndata") do
    memory = MemoryDaf(; name = "memory!")

    add_axis!(memory, "cell", ["A", "B"])
    add_axis!(memory, "gene", ["X", "Y", "Z"])

    set_matrix!(memory, "cell", "gene", "UMIs", [0 1 2; 3 4 5]; relayout = true)
    set_matrix!(memory, "gene", "cell", "LogUMIs", log2.(transpose([0 1 2; 3 4 5] .+ 1)))
    set_scalar!(memory, "X_is", "UMIs")

    set_vector!(memory, "cell", "age", [0, 1])

    set_matrix!(memory, "cell", "cell", "edges", [0 1; 2 3])

    nested_test("daf_as_anndata") do
        adata = daf_as_anndata(memory; obs_is = "cell", var_is = "gene")
        test_adata(adata)
        return nothing
    end

    nested_test("anndata_as_daf") do
        nested_test("name") do
            adata = daf_as_anndata(memory; obs_is = "cell", var_is = "gene")
            test_adata(adata)
            back = anndata_as_daf(adata; name = "back!")
            @test back.name == "back!"
            test_daf(back)
            return nothing
        end

        nested_test("!name") do
            adata = daf_as_anndata(memory; obs_is = "cell", var_is = "gene")
            test_adata(adata)
            back = anndata_as_daf(adata)
            @test back.name == "anndata"
            test_daf(back)
            return nothing
        end

        nested_test("+name") do
            set_scalar!(memory, "name", "anndata!")
            adata = daf_as_anndata(memory; obs_is = "cell", var_is = "gene")
            test_adata(adata)
            back = anndata_as_daf(adata)
            @test back.name == "anndata!"
            test_daf(back)
            return nothing
        end

        nested_test("unsupported") do
            adata = daf_as_anndata(memory; obs_is = "cell", var_is = "gene")
            test_adata(adata)

            nested_test("mapping") do
                adata.uns["mapping"] = Dict("a" => 1)

                nested_test("ignore") do
                    back = anndata_as_daf(adata; unsupported_handler = IgnoreHandler)
                    test_daf(back)
                    return nothing
                end

                nested_test("warn") do
                    back = @test_logs min_level = Logging.Warn (
                        :warn,
                        chomp(
                            """
                      unsupported type: Dict{String, Int64}
                      of the property: uns[mapping]
                      supported type is: Union{Bool, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8, S} where S<:AbstractString
                      in AnnData for the daf data: anndata
                      """,
                        ),
                    ) anndata_as_daf(adata; unsupported_handler = WarnHandler)
                    test_daf(back)
                    return nothing
                end

                nested_test("error") do
                    @test_throws chomp(
                        """
                  unsupported type: Dict{String, Int64}
                  of the property: uns[mapping]
                  supported type is: Union{Bool, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8, S} where S<:AbstractString
                  in AnnData for the daf data: anndata
                  """,
                    ) anndata_as_daf(adata; unsupported_handler = ErrorHandler)
                end
            end

            nested_test("unknown_axis") do
                adata.obsm["unknown_axis"] = [0 1 3; 4 5 6]

                nested_test("ignore") do
                    back = anndata_as_daf(adata; unsupported_handler = IgnoreHandler)
                    test_daf(back)
                    return nothing
                end

                nested_test("warn") do
                    back = @test_logs min_level = Logging.Warn (:warn, chomp("""
                                                                       unsupported annotation: obsm[unknown_axis]
                                                                       in AnnData for the daf data: anndata
                                                                       """)) anndata_as_daf(
                        adata;
                        unsupported_handler = WarnHandler,
                    )
                    test_daf(back)
                    return nothing
                end

                nested_test("error") do
                    @test_throws chomp("""
                                 unsupported annotation: obsm[unknown_axis]
                                 in AnnData for the daf data: anndata
                                 """) anndata_as_daf(adata; unsupported_handler = ErrorHandler)
                end
            end
        end
    end

    nested_test("file") do
        mktempdir() do path
            adata = daf_as_anndata(memory; obs_is = "cell", var_is = "gene", h5ad = "$(path)/test.h5ad")
            test_adata(adata)
            back = anndata_as_daf("$(path)/test.h5ad"; unsupported_handler = ErrorHandler)
            test_daf(back)
            return nothing
        end
    end
end
