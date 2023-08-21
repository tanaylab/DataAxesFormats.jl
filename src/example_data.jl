"""
Example data for doctest tests.
"""
module ExampleData

using Daf.Data
using Daf.MemoryFormat
using Random

function random_entries(gen::MersenneTwister, size::Int, entry_names::Vector{String})::Vector{String}
    return entry_names[round.(Int8, rand(gen, size) * length(entry_names) .+ 0.5)]
end

"""
Create an example memory `daf` to use for doctest tests.
"""
function example_daf()::MemoryDaf
    storage = MemoryDaf("example!")

    set_scalar!(storage, "version", "1.0")

    gene_names = ["RSPO3", "FOXA1", "WNT6", "TNNI1", "MSGN1", "LMO2", "SFRP5", "DLX5", "ITGA4", "FOXA2"]
    module_names = ["M$(index)" for index in 1:3]
    cell_names = ["C$(index)" for index in 1:20]
    batch_names = ["B$(index)" for index in 1:4]
    invalid_batch_names = copy(batch_names)
    invalid_batch_names[1] = "I1"
    partial_batch_names = copy(batch_names)
    partial_batch_names[1] = ""

    type_names = ["T$(index)" for index in 1:3]

    gen = MersenneTwister(123)

    add_axis!(storage, "gene", gene_names)
    add_axis!(storage, "module", module_names)
    add_axis!(storage, "cell", cell_names)
    add_axis!(storage, "batch", batch_names)
    add_axis!(storage, "type", type_names)

    set_matrix!(
        storage,
        "cell",
        "gene",
        "UMIs",
        round.(Int16, randexp(gen, length(cell_names), length(gene_names)) * 10);
    )
    set_vector!(storage, "cell", "batch", random_entries(gen, length(cell_names), batch_names))
    set_vector!(storage, "cell", "batch.invalid", random_entries(gen, length(cell_names), invalid_batch_names))
    set_vector!(storage, "cell", "type", random_entries(gen, length(cell_names), type_names))

    set_vector!(storage, "batch", "sex", random_entries(gen, length(batch_names), ["Male", "Female"]))
    set_vector!(storage, "batch", "age", round.(Int8, rand(gen, length(batch_names)) * 4 .+ 0.5))

    set_vector!(storage, "type", "color", ["red", "green", "blue"])

    set_vector!(storage, "gene", "module", random_entries(gen, length(gene_names), module_names))
    set_vector!(storage, "gene", "marker", rand(gen, Bool, length(gene_names)))
    set_vector!(storage, "gene", "noisy", rand(gen, Bool, length(gene_names)))
    set_vector!(storage, "gene", "lateral", rand(gen, Bool, length(gene_names)))

    set_vector!(storage, "cell", "batch.partial", random_entries(gen, length(cell_names), partial_batch_names))

    return storage
end

end # module
