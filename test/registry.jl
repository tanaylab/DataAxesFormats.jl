import Daf.Registry.EltwiseOperation
import Daf.Registry.@query_operation
import Daf.Registry.ReductionOperation

struct TestEltwiseOp <: EltwiseOperation
    dtype::Union{Type, Nothing}
end
@query_operation TestEltwiseOp

struct TestReductionOp <: ReductionOperation
    dtype::Union{Type, Nothing}
end
@query_operation TestReductionOp

struct InvalidEltwiseOp <: EltwiseOperation end

test_set("registry") do
    test_set("register query operation") do
        @test_throws dedent("""
            conflicting registrations for the eltwise operation: TestEltwiseOp
            1st in: $(@__FILE__):8
            2nd in: $(@__FILE__):-1
        """) register_query_operation(TestEltwiseOp, @__FILE__, -1)

        @test_throws dedent("""
            conflicting registrations for the reduction operation: TestReductionOp
            1st in: $(@__FILE__):13
            2nd in: $(@__FILE__):-1
        """) register_query_operation(TestReductionOp, @__FILE__, -1)

        @test_throws dedent("""
            missing field: dtype
            for the eltwise operation: InvalidEltwiseOp
            in: $(@__FILE__):-1
        """) register_query_operation(InvalidEltwiseOp, @__FILE__, -1)
    end
end
