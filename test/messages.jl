using Daf
using LinearAlgebra
using SparseArrays

test_set("messages") do
    test_set("unique_name") do
        @test unique_name("foo") == "foo#1"
        @test unique_name("foo") == "foo#2"
    end

    test_set("present") do
        @test present(missing) == "missing"
        @test present("foo") == "\"foo\""
        @test present(:foo) == ":foo"
        @test present(1) == "1"
        @test present(1.0) == "1.0"
        @test present(vec([0 1 2])) == "3 x Int64 (dense)"
        @test present(SparseVector(vec([0 1 2]))) == "3 x Int64 (sparse 67%)"
        @test present([0 1 2; 3 4 5]) == "2 x 3 x Int64 (dense, column-major)"
        @test present(transpose([0 1 2; 3 4 5])) == "3 x 2 x Int64 (dense, row-major)"
        @test present(SparseMatrixCSC([0 1 2; 3 4 5])) == "2 x 3 x Int64 (sparse 83%, column-major)"
        @test present(transpose(SparseMatrixCSC([0 1 2; 3 4 5]))) == "3 x 2 x Int64 (sparse 83%, row-major)"
        @test present(zeros(1, 2, 3)) == "1 x 2 x 3 x Float64 (Array{Float64, 3})"
    end
end
