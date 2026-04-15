nested_test("keys") do
    nested_test("pairs_as_dict") do
        nested_test("dict") do
            dict = Dict("a" => 1, "b" => 2)
            @test pairs_as_dict(dict) === dict
        end

        nested_test("vector") do
            result = pairs_as_dict(["a" => 1, "b" => 2])
            @test result isa Dict{Any, Any}
            @test result["a"] == 1
            @test result["b"] == 2
        end

        nested_test("named_tuple") do
            result = pairs_as_dict((; a = 1, b = 2))
            @test result isa Dict{AbstractString, Any}
            @test result["a"] == 1
            @test result["b"] == 2
        end

        nested_test("nothing") do
            @test pairs_as_dict(nothing) === nothing
        end
    end
end
