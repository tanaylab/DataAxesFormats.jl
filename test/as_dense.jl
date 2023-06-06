using Base: elsize
using Daf
using LinearAlgebra
using SparseArrays

function test_same_strided_matrix_content(left::AbstractMatrix, right::AbstractMatrix)
    @test strides(left) == strides(right)
    @test stride(left, 1) == stride(right, 1)
    @test stride(left, 2) == stride(right, 2)
    return test_same_matrix_content(left, right)
end

function test_same_matrix_content(left::AbstractMatrix, right::AbstractMatrix)
    @test length(left) == length(right)
    @test size(left) == size(right)
    @test left == right
end

function is_same_matrix_storage(left::AbstractMatrix, right::AbstractMatrix)::Bool
    @assert left[1, 1] == right[1, 1]
    left[1, 1] += 1
    result = left[1, 1] == right[1, 1]
    left[1, 1] -= 1
    @assert left[1, 1] == right[1, 1]
    return result
end

test_set("as_dense") do
    test_set("array") do
        array = [0 1 2; 3 4 5]
        @test array isa DenseArray
        @test as_dense_if_possible(array) === array
        @test as_dense_or_copy(array) === array
        @test as_dense_or_fail(array) === array
    end
    test_set("transpose") do
        array = transpose([0 1 2; 3 4 5])
        @test !(array isa DenseArray)
        @test as_dense_if_possible(array) isa DenseArray
        @test as_dense_or_copy(array) isa DenseArray
        @test as_dense_or_fail(array) isa DenseArray
        @test as_dense_if_possible(array) !== array
        @test as_dense_or_copy(array) !== array
        @test as_dense_or_fail(array) !== array
        test_same_strided_matrix_content(as_dense_if_possible(array), array)
        test_same_strided_matrix_content(as_dense_or_copy(array), array)
        test_same_strided_matrix_content(as_dense_or_fail(array), array)
        @test is_same_matrix_storage(array, as_dense_if_possible(array))
        @test is_same_matrix_storage(array, as_dense_or_copy(array))
        @test is_same_matrix_storage(array, as_dense_or_fail(array))
    end
    test_set("sparse") do
        array = SparseMatrixCSC([0 1 2; 3 4 5])
        @test !(array isa DenseArray)
        @test as_dense_if_possible(array) === array
        @test as_dense_or_copy(array) isa DenseArray
        @test_throws "the array: SparseMatrixCSC{Int64, Int64} is not dense" as_dense_or_fail(array)
        @test as_dense_or_copy(array) !== array
        test_same_matrix_content(array, as_dense_or_copy(array))
        @test !is_same_matrix_storage(array, as_dense_or_copy(array))
    end
    test_set("sparse transpose") do
        array = transpose(SparseMatrixCSC([0 1 2; 3 4 5]))
        @test !(array isa DenseArray)
        @test as_dense_if_possible(array) === array
        @test as_dense_or_copy(array) isa DenseArray
        @test_throws "the array: Transpose{Int64, SparseMatrixCSC{Int64, Int64}} is not dense" as_dense_or_fail(array)
        @test as_dense_or_copy(array) !== array
        test_same_matrix_content(array, as_dense_or_copy(array))
        @test !is_same_matrix_storage(array, as_dense_or_copy(array))
    end
end
