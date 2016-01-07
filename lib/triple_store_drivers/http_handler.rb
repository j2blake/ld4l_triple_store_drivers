module TripleStoreDrivers
  module HttpHandler
    def http_get(url, block, params, headers={})
      uri = URI.parse(url)

      unless params.empty?
        joined_params = params.to_a.map {|p| p[0] + '=' + p[1]}.join('&')
        if uri.query.empty?
          uri.query = joined_params
        else
          uri.query = uri.query + '&' + joined_params
        end
      end

      Net::HTTP.start(uri.host, uri.port) do |http|
        http.read_timeout = 180

        request = Net::HTTP::Get.new(uri.request_uri)
        headers.each_pair do |k, v|
          request[k] = v
        end

        begin
          http.request(request) do |response|
            if response.code == "301"
              follow_redirect
              redirect = Net::HTTP::Post.new(URI.parse(response.header['location']).request_uri)
              headers.each_pair do |k, v|
                redirect[k] = v
              end
              response = http.request(redirect)
            end

            if response.code == "200"
              block.call(response) if block
            else
              if block_given?
                yield response
              else
                response.value
              end
            end
          end
        rescue IOError => e
          raise e.exception(e.message << "\nProblem request: \n#{inspect_request(request, url)}")
        end
      end
    end

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
            if response.code == "301"
              redirect = Net::HTTP::Post.new(URI.parse(response.header['location']).request_uri)
              redirect.set_form_data(params)
              headers.each_pair do |k, v|
                redirect[k] = v
              end
              response = http.request(redirect)
            end

            if response.code == "200"
              block.call(response) if block
            else
              if block_given?
                yield response
              else
                response.value
              end
            end
          end
        rescue IOError => e
          raise e.exception(e.message << "\nProblem request: \n#{inspect_request(request, url)}")
        end
      end
    end

    def inspect_request(r, url)
      headers = r.to_hash.to_a.map{|h| "   #{h[0]} ==> #{h[1]}"}.join("\n")
      body = CGI.unescape(r.body)
      "#{r.method} #{url}\n#{headers}\n#{body}"
    end

  end
end