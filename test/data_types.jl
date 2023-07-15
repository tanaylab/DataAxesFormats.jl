import Daf.DataTypes.require_storage_matrix
import Daf.DataTypes.require_storage_vector

test_set("data_types") do
    test_set("require_storage_matrix") do
        @test_throws "type: Int64 is not a valid Daf.StorageMatrix" require_storage_matrix(1)
        require_storage_matrix(rand(4, 4))
        require_storage_matrix(sprand(4, 4, 0.5))
        @test_throws "type: Transpose{Float64, Matrix{Float64}} is not a valid Daf.StorageMatrix" require_storage_matrix(
            transpose(rand(4, 4)),
        )
        @test_throws "type: Transpose{Float64, SparseMatrixCSC{Float64, Int64}} is not a valid Daf.StorageMatrix" require_storage_matrix(
            transpose(sprand(4, 4, 0.5)),
        )
    end

    test_set("require_storage_vector") do
        require_storage_vector(as_dense_or_fail(rand(4, 4)[:, 1]))
        require_storage_vector(as_dense_or_fail(rand(4, 4)[1, :]))
        require_storage_vector(sprand(4, 4, 0.5)[:, 1])
        require_storage_vector(sprand(4, 4, 0.5)[1, :])
        require_storage_vector(as_dense_or_fail(selectdim(rand(4, 4), Columns, 2)))
        @test_throws "type: SubArray{Float64, 1, Matrix{Float64}, Tuple{Int64, Base.Slice{Base.OneTo{Int64}}}, true} " *
                     "is not a valid Daf.StorageVector" require_storage_vector(
            as_dense_if_possible(selectdim(rand(4, 4), Rows, 2)),
        )
        @test_throws "type: Int64 is not a valid Daf.StorageVector" require_storage_vector(1)
    end

    test_set("as_storage") do
        test_set("array") do
            array = [0 1 2; 3 4 5]
            @test array isa StorageMatrix
            @test as_storage_if_possible(array) === array
            @test as_storage_or_copy(array) === array
            @test as_storage_or_fail(array) === array
        end
        test_set("transpose") do
            array = transpose([0 1 2; 3 4 5])
            @test !(array isa StorageMatrix)
            @test as_storage_if_possible(array) isa DenseMatrix
            @test as_storage_or_copy(array) isa DenseMatrix
            @test as_storage_or_fail(array) isa DenseMatrix
        end
        test_set("sparse") do
            array = SparseMatrixCSC([0 1 2; 3 4 5])
            @test array isa StorageMatrix
            @test as_storage_if_possible(array) === array
            @test as_storage_or_copy(array) === array
            @test as_storage_or_fail(array) === array

            array = array[1, :]
            @test array isa StorageVector
            @test as_storage_if_possible(array) === array
            @test as_storage_or_copy(array) === array
            @test as_storage_or_fail(array) === array
        end
        test_set("sparse transpose") do
            array = transpose(SparseMatrixCSC([0 1 2; 3 4 5]))
            @test !(array isa StorageMatrix)
            @test as_storage_if_possible(array) === array
            @test as_storage_or_copy(array) isa SparseMatrixCSC
            @test_throws "the array: Transpose{Int64, SparseMatrixCSC{Int64, Int64}} is not storage" as_storage_or_fail(
                array,
            )
        end
    end
end
