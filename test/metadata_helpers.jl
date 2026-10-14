# Verify that the metadata of a vector property matches its data.
function verify_vector_metadata(
    daf::DafReader,
    axis::AbstractString,
    name::AbstractString;
    is_packed::Bool = false,
)::Nothing
    verify_metadata(vector_metadata(daf, axis, name), get_vector(daf, axis, name); is_packed)
    return nothing
end

# Verify that the metadata of a matrix property matches its data, in the layout it is stored in.
function verify_matrix_metadata(
    daf::DafReader,
    rows_axis::AbstractString,
    columns_axis::AbstractString,
    name::AbstractString;
    is_packed::Bool = false,
)::Nothing
    verify_metadata(
        matrix_metadata(daf, rows_axis, columns_axis, name),
        get_matrix(daf, rows_axis, columns_axis, name; relayout = false);
        is_packed,
    )
    return nothing
end

# A sparse string property is read back dense, so its storage is checked against its non-empty values.
function verify_metadata(metadata::PropertyMetadata, values::AbstractArray; is_packed::Bool)::Nothing
    values = base_array(values)
    if eltype(values) <: AbstractString
        @test metadata.eltype == String
        if metadata.is_sparse
            @test metadata.nnz == count(!isempty, values)
        end
    else
        @test metadata.eltype == eltype(values)
        @test metadata.is_sparse == issparse(values)
        if metadata.is_sparse
            @test metadata.nnz == nnz(values)
            @test metadata.indtype == SparseArrays.indtype(values)
        end
    end
    if !metadata.is_sparse
        @test metadata.nnz === nothing
        @test metadata.indtype === nothing
    end
    @test metadata.is_packed == is_packed
    return nothing
end
