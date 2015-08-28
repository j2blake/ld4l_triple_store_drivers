module TripleStoreDrivers
  module HttpHandler
    def http_post(url, block, params, headers={})
      uri = URI.parse(url)

      Net::HTTP.start(uri.host, uri.port) do |http|
        http.read_timeout = 180

        request = Net::HTTP::Post.new(uri.request_uri)
        request.set_form_data(params)
        headers.each_pair do |k, v|
          request[k] = v
        end

        begin
          http.request(request) do |response|
            response.value
            block.call(response) if block
          end
        rescue IOError => e
          raise e.exception(e.message << "\nProblem request: \n#{inspect_request(request, url)}")
        end
      end
    end
  end
end