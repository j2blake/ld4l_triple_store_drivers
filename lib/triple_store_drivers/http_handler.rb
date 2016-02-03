module TripleStoreDrivers
  module HttpHandler
    attr_reader :http_counts
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
              response_status_summary(reason: :good, method: :get, request: uri.request_uri)
              block.call(response) if block
            else
              response_status_summary(reason: :bad_code, method: :get, request: uri.request_uri, code: response.code)
              if block_given?
                yield response
              else
                response.value
              end
            end
          end
        rescue IOError => e
          response_status_summary(reason: :io_error, method: :get, exception: e, request: uri.request_uri)
          raise e.exception(e.message << "\nProblem request: \n#{inspect_request(request, url)}")
        rescue
          response_status_summary(reason: :error, method: :get, exception: $!, request: uri.request_uri)
          raise $!
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
              response_status_summary(reason: :good, method: :put, request: uri.request_uri)
              block.call(response) if block
            else
              response_status_summary(reason: :bad_code, method: :put, request: uri.request_uri, code: response.code)
              if block_given?
                yield response
              else
                response.value
              end
            end
          end
        rescue IOError => e
          response_status_summary(reason: :io_error, method: :put, exception: e, request: uri.request_uri)
          raise e.exception(e.message << "\nProblem request: \n#{inspect_request(request, url)}")
        rescue
          response_status_summary(reason: :error, method: :put, exception: $!, request: uri.request_uri)
          raise $!
        end
      end
    end

    def response_status_summary(props)
      @http_counts = Hash.new {|h, k| h[k] = Hash.new {|h, k| h[k] = 0}} unless @http_counts
      begin
        method = [:get, :put].find() {|m| m == props[:method]} || :other
        reason = [:good, :bad_code, :io_error, :error].find() {|r| r == props[:reason]} || :other
        @http_counts[method][reason] += 1
        puts "response_status_summary: %s, this call %s" % [@http_counts.inspect, props.inspect] unless props[:reason] == :good
      rescue
        puts "bogus call to response_status_summary: %s, this call %s" % [@http_counts.inspect, props.inspect]
        puts $!
        puts $!.backtrace.join('\n')
      end
    end

    def inspect_request(r, url)
      headers = r.to_hash.to_a.map{|h| "   #{h[0]} ==> #{h[1]}"}.join("\n")
      body = CGI.unescape(r.body)
      "#{r.method} #{url}\n#{headers}\n#{body}"
    end

  end
end