# TODO: Using `REPL` magically changes `@doc` to return `Markdown.MD`. Up to Julia 1.10 we could just call `doc(foo)`
# and get the Markdown string. As of Julia 1.11 we need to do this instead. Sigh. There's probably a better way but life
# is too short.
using REPL
using Markdown
function formatdoc(md::Markdown.MD)::String
    return dedent(repr("text/markdown", md)) * "\n"
end

"""
None

Just a function with a default `x` of $(DEFAULT.x).
"""
@documented function none(; x = 1)
    return x
end

SINGLE_CONTRACT = Contract(;
    axes = [
        "cell" => (RequiredInput, "The sampled single cells."),
        "gene" => (RequiredInput, "The sampled genes."),
        "block" => (OptionalOutput, "Blocks of cells."),
    ],
    data = [
        "version" => (OptionalInput, String, "In major.minor.patch format."),
        "quality" => (GuaranteedOutput, Float64, "Overall output quality score between 0.0 and 1.0."),
        ("gene", "noisy") => (OptionalInput, Bool, "Mask of genes with high variability."),
        ("cell", "special") => (OptionalOutput, Bool, "Computed mask of special cells, if requested."),
        ("cell", "gene", "UMIs") =>
            (RequiredInput, Union{UInt8, UInt16, UInt32, UInt64}, "The number of sampled scRNA molecules."),
        ("block", "cell", "gene", "match") =>
            (OptionalOutput, StorageFloat, "How well the gene of the cell match the block."),
    ],
)

"""
Single

The `quality` is mandatory. The default `optional` is $(DEFAULT.optional). The default `named` is $(DEFAULT.named).

$(CONTRACT)
"""
@computation SINGLE_CONTRACT @logged function single(
    daf::DafWriter,
    quality::Float64,
    optional::Int = 1;
    named::AbstractString = "Foo",
)::Nothing
    get_matrix(daf, "cell", "gene", "UMIs")
    set_scalar!(daf, "quality", quality)
    return nothing
end

"""
Dual

# First

$(CONTRACT1)

# Second

$(CONTRACT2)
"""
@logged @computation Contract(
    data = [
        "version" => (RequiredInput, String, "In major.minor.patch format."),
        "quality" => (GuaranteedOutput, Float64, "Overall output quality score between 0.0 and 1.0."),
    ],
) Contract(
    data = [
        "version" => (GuaranteedOutput, String, "In major.minor.patch format."),
        "quality" => (RequiredInput, Float64, "Overall output quality score between 0.0 and 1.0."),
    ],
) function cross(first::DafWriter, second::DafWriter; overwrite::Bool = false)::Nothing
    set_scalar!(second, "version", get_scalar(first, "version"); overwrite = overwrite)
    set_scalar!(first, "quality", get_scalar(second, "quality"); overwrite = overwrite)
    return nothing
end

"""
Missing

$(CONTRACT)
"""
function missing_single(daf::DafWriter)::Nothing
    return nothing
end

"""
Relaxed

$(CONTRACT)
"""
@computation Contract(is_relaxed = true) function relaxed(daf::DafWriter)::Nothing  # untested
    return nothing
end

"""
Missing

$(DEFAULT.x)
"""
@computation Contract() function missing_default(daf::DafWriter, x::Int)::Nothing  # untested
    return nothing
end

"""
Missing

$(CONTRACT1)

$(CONTRACT2)
"""
function missing_both(first::DafWriter, second::DafWriter)::Nothing  # untested
    return nothing
end

"""
Missing

$(CONTRACT1)

$(CONTRACT2)
"""
@computation Contract() function missing_second(first::DafWriter, second::DafWriter)::Nothing  # untested
    return nothing
end

function not_computation()::Nothing  # untested
    return nothing
end

nested_test("computations") do
    nested_test("access") do
        nested_test("!contract") do
            @test_throws "not a @documented or @computation function: Main.not_computation" function_contract(
                not_computation,
            )
        end

        nested_test("contract") do
            @test function_contract(single) === SINGLE_CONTRACT
        end

        nested_test("default") do
            @test function_default(single, :optional) == 1
        end

        nested_test("!default") do
            @test_throws dedent("""
                no parameter with default: quality
                exists for the function: Main.single
                """) function_default(single, :quality)
        end
    end

    nested_test("none") do
        nested_test("default") do
            @test none() == 1
        end

        nested_test("parameter") do
            @test none(; x = 2) == 2
        end

        nested_test("docs") do
            @test formatdoc(@doc none) == dedent("""
                                                 None

                                                 Just a function with a default `x` of `1`.
                                                 """) * "\n"
        end
    end

    nested_test("single") do
        daf = MemoryDaf(; name = "memory!")

        nested_test("()") do
            add_axis!(daf, "cell", ["A", "B"])
            add_axis!(daf, "gene", ["X", "Y", "Z"])
            set_matrix!(daf, "cell", "gene", "UMIs", UInt8[0 1 2; 3 4 5])
            @test single(daf, 0.0) === nothing
        end

        nested_test("missing") do
            @test_throws dedent("""
                missing input axis: gene
                for the computation: Main.single
                on the daf data: memory!
            """) single(daf, 0.0)
        end

        nested_test("docs") do
            @test formatdoc(@doc single) ==
                  dedent(
                """
                Single

                The `quality` is mandatory. The default `optional` is `1`. The default `named` is `\"Foo\"`.

                ## Inputs

                ### Scalars

                **version**::String (optional): In major.minor.patch format.

                ### Axes

                **cell** (required): The sampled single cells.

                **gene** (required): The sampled genes.

                ### Vectors

                **gene @ noisy**::Bool (optional): Mask of genes with high variability.

                ### Matrices

                **cell, gene @ UMIs**::Union{UInt16, UInt32, UInt64, UInt8} (required): The number of sampled scRNA molecules.

                ## Outputs

                ### Scalars

                **quality**::Float64 (guaranteed): Overall output quality score between 0.0 and 1.0.

                ### Axes

                **block** (optional): Blocks of cells.

                ### Vectors

                **cell @ special**::Bool (optional): Computed mask of special cells, if requested.

                ### Tensors

                **block; cell, gene @ match**::Union{Float32, Float64} (optional): How well the gene of the cell match the block.
                """,
            ) * "\n"
        end

        nested_test("relaxed") do
            @test formatdoc(@doc relaxed) ==
                  dedent("""
                         Relaxed

                         ## Inputs

                         Additional inputs may be used depending on the parameter(s).

                         ## Outputs

                         Additional outputs may be created depending on the parameter(s).
                         """) * "\n"
        end

        nested_test("!docs") do
            @test missing_single(daf) === nothing
            @test_throws dedent("""
                no contract(s) associated with: Main.missing_single
                use: @computation Contract(...) function Main.missing_single(...)
            """) formatdoc(@doc missing_single)
        end
    end

    nested_test("cross") do
        first = MemoryDaf(; name = "first!")
        second = MemoryDaf(; name = "second!")

        nested_test("()") do
            set_scalar!(first, "version", "0.0")
            set_scalar!(second, "quality", 1.0)
            @test cross(first, second) === nothing
            @test get_scalar(first, "quality") == 1.0
            @test get_scalar(second, "version") == "0.0"
        end

        nested_test("overwrite") do
            set_scalar!(first, "version", "0.0")
            set_scalar!(first, "quality", 0.0)
            set_scalar!(second, "quality", 1.0)
            set_scalar!(second, "version", "1.0")
            @test cross(first, second; overwrite = true) === nothing
            @test get_scalar(first, "quality") == 1.0
            @test get_scalar(second, "version") == "0.0"
        end

        nested_test("!overwrite") do
            set_scalar!(first, "version", "0.0")
            set_scalar!(first, "quality", 0.0)
            set_scalar!(second, "quality", 1.0)
            set_scalar!(second, "version", "1.0")
            @test_throws dedent("""
                pre-existing GuaranteedOutput scalar: quality
                for the computation: Main.cross.1
                on the daf data: first!
            """) cross(first, second)
            @test get_scalar(first, "quality") == 0.0
            @test get_scalar(second, "version") == "1.0"
        end

        nested_test("missing") do
            nested_test("first") do
                set_scalar!(second, "quality", 0.0)
                @test_throws dedent("""
                    missing input scalar: version
                    with type: String
                    for the computation: Main.cross.1
                    on the daf data: first!
                """) cross(first, second)
            end

            nested_test("second") do
                set_scalar!(first, "version", "0.0")
                @test_throws dedent("""
                    missing input scalar: quality
                    with type: Float64
                    for the computation: Main.cross.2
                    on the daf data: second!
                """) cross(first, second)
            end
        end

        nested_test("docs") do
            @test formatdoc(@doc cross) == dedent("""
                Dual

                # First

                ## Inputs

                ### Scalars

                **version**::String (required): In major.minor.patch format.

                ## Outputs

                ### Scalars

                **quality**::Float64 (guaranteed): Overall output quality score between 0.0 and 1.0.

                # Second

                ## Inputs

                ### Scalars

                **quality**::Float64 (required): Overall output quality score between 0.0 and 1.0.

                ## Outputs

                ### Scalars

                **version**::String (guaranteed): In major.minor.patch format.
            """) * "\n"
        end

        nested_test("!doc1") do
            @test_throws dedent("""
                no contract(s) associated with: Main.missing_both
                use: @computation Contract(...) function Main.missing_both(...)
            """) formatdoc(@doc missing_both)
        end

        nested_test("!doc2") do
            @test_throws dedent("""
                no second contract associated with: Main.missing_second
                use: @computation Contract(...) Contract(...) function Main.missing_second(...)
            """) formatdoc(@doc missing_second)
        end

        nested_test("!default") do
            @test_throws dedent("""
                no default for a parameter: x
                in the computation: Main.missing_default
            """) formatdoc(@doc missing_default)
        end
    end
end
