# Build an HTTP response for `bytes`, honoring an optional `Range: bytes=N-M` or `Range: bytes=-N` suffix header
# on `request` with a `206 Partial Content` reply (otherwise `200 OK`). Used by the test-only static-server
# handlers so the striped + packed HTTP read paths in `HttpDaf` / `ZarrDaf` see real `Range` slicing.
function respond_with_range(bytes::Vector{UInt8}, request)::HTTP.Response
    range_header = HTTP.header(request, "Range", "")
    if isempty(range_header)
        return HTTP.Response(200, bytes)
    end
    @assert startswith(range_header, "bytes=") "unexpected Range header: $(range_header)"
    spec = SubString(range_header, length("bytes=") + 1)
    n_bytes = length(bytes)
    if startswith(spec, "-")
        suffix_n = parse(Int, SubString(spec, 2))
        first_index = n_bytes - suffix_n + 1
        last_index = n_bytes
    else
        dash_index = findfirst(==('-'), spec)
        @assert dash_index !== nothing "unexpected Range header: $(range_header)"
        first_index = parse(Int, SubString(spec, 1, dash_index - 1)) + 1
        last_index = parse(Int, SubString(spec, dash_index + 1)) + 1
    end
    @assert 1 <= first_index <= last_index <= n_bytes "out-of-range Range header: $(range_header) for $(n_bytes) bytes"
    content_range = "bytes $(first_index - 1)-$(last_index - 1)/$(n_bytes)"
    return HTTP.Response(206, ["Content-Range" => content_range], bytes[first_index:last_index])
end
