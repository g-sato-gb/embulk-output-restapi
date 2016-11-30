require 'faraday'
require 'json'
require 'jsonpath'
require 'csv'
require 'pp'
require 'mail'

module Embulk
  module Output

    class Restapi < OutputPlugin
      Plugin.register_output("restapi", self)

      def self.transaction(config, schema, count, &control)
        task = {
          'base_url' => config.param('base_url', :string, default: nil),
          'path' => config.param('path', :string, default: nil),
          'method' => config.param('method', :string, default: 'post'),
          'output_path' => config.param('output_path', :string, default: '.\\send'),
          'output_file' => config.param('output_file', :string, default: 'product'),
          'headers' => config.param('headers', :array, default: []),
          'params' => config.param('params', :array, default: []),
          'mailer' => config.param('mailer', :hash, default: {}),
        }
        resume(task, schema, count, &control)
      end

      def self.resume(task, schema, count, &control)
        task_reports = yield(task)
        next_config_diff = {}
        return next_config_diff
      end

      def init
      end

      def close
      end

      def add(page)
        log = Embulk::logger
        log.info("↓↓↓↓↓↓↓↓↓↓ 【restapi】 start ")
        begin
          csv_data = CSV.generate(headers: schema.names,
                                  write_headers: true,
                                  force_quotes: true) do |csv|
            page.each do |record|
              csv << record
            end
          end
          upload_file = @task['output_path']+'\\'+
                        @task['output_file']+'_'+
                        Time.now.strftime("%Y%m%d%H%M%S")+".csv"
          log.info("【restapi】 uploadfile : #{upload_file}")
          File.open(upload_file, 'w') do |file|
            file.write(csv_data)
          end
          @response = request(upload_file)
          if @response
            res_hash = JSON.parse(@response)
            log.info("response json \n" + JSON.pretty_generate(res_hash))
            if res_hash["error"] > 0
              log.info("!!ERROR!!")
              # エラーメール送信
              send_mail task['mailer'] do |mail|
                mail.body = PP.pp(res_hash,'')
              end
              log.info("==ERROR   END==")
            elsif
              File.delete upload_file
              log.info("==SUCCESS END==")
            end
          end
        rescue => e
          log.error("!!!!!!!!!!!!error!!!!!!!!!!!! : #{e.message} \n" + e.backtrace.join("\n"))
          # エラーメール送信
          send_mail task['mailer'] do |mail|
            mail.body = PP.pp("#{e.message} \n",'')
          end
          log.info("==ERROR   END==")
        end
        log.info("↑↑↑↑↑↑↑↑↑↑ 【restapi】 end ")
      end

      def finish
      end

      def abort
      end

      def commit
        task_report = {}
        task_report = JSON.parse(@response) if @response
        return task_report
      end

      def request(csv_file)
        conn = Faraday.new(:url => @task[:base_url]) do |builder|
          builder.request :multipart
          builder.request :url_encoded
          builder.adapter :net_http
        end
        body_param = {
          csv_file: Faraday::UploadIO.new(csv_file, 'application/vnd.ms-excel')
        }
        @task[:params].each do |param|
          body_param[param["name"]] = param["value"]
        end
        response = conn.post do |req|
          req.url @task[:path]
          req.body = body_param
          @task[:headers].each do |head|
            req.headers[head["name"]] = head["value"]
          end
        end
        body_param[:csv_file].close
        response.body
      end

      def send_mail(param, &body_edit)
        mail = Mail.new
        param['mail'].each {|k,v| mail.send(k,v)} 
        body_edit.call(mail) if body_edit
        mail.delivery_method :smtp, param['smtp'].map{|k,v| [k.to_sym, v] }.to_h
        mail.deliver!
      end
    end
  end
end
