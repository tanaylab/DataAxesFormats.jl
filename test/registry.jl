import DafJL.Registry.EltwiseOperation
import DafJL.Registry.@query_operation
import DafJL.Registry.ReductionOperation

struct TestEltwiseOp <: EltwiseOperation end
@query_operation TestEltwiseOp

struct TestReductionOp <: ReductionOperation end
@query_operation TestReductionOp

nested_test("registry") do
    nested_test("conflicting") do
        nested_test("eltwise") do
            @test_throws dedent("""
                conflicting registrations for the eltwise operation: TestEltwiseOp
                first in: $(@__FILE__):6
                second in: $(@__FILE__):-1
            """) register_query_operation(TestEltwiseOp, @__FILE__, -1)
        end

        nested_test("reduction") do
            @test_throws dedent("""
                conflicting registrations for the reduction operation: TestReductionOp
                first in: $(@__FILE__):9
                second in: $(@__FILE__):-1
            """) register_query_operation(TestReductionOp, @__FILE__, -1)
        end
    end
end
