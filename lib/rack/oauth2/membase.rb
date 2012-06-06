# -*- encoding : utf-8 -*-

require 'dalli'

module Rack
  module OAuth2
    class Server
      def self.database
        @database ||= Server.options.database                                                                       
        raise "No database Configured. You must configure it using Server.options.database = Rack::OAuth2::Membase.new()[db_name]" unless @database 
        raise "You set Server.database to #{Server.database.class}, should be a Rack::OAuth2::Membase object" unless Rack::OAuth2::Membase === @database 
        @database
      end
    end

    class Membase < Dalli::Client
      def initialize(opts = {})
        super(opts[:url], :username => opts[:username],
             :password => opts[:password])
      end

      def group(query)
      end

      def insert(toinsert, opts = {})
        id = 0
        lock do
          elems = get(@keyname) || []
          id = toinsert[:_id] || BSON::ObjectId.new
          toinsert.merge!({:_id => id.to_s})
          elems << toinsert
          set(@keyname, elems)
        end

        id.to_s
      end

      def update(tofind, toupdate, opts = {})
        lock do
          elems = get(@keyname) || []
          found = elems.reject { |elem| (elem.to_a - tofind.to_a).any? { |k,_|
              toupdate.has_key?(k)
            }
          }
          found.map { |elem|
            elems.delete(elem)
            newelem = if toupdate.kind_of?(Hash)
              if toupdate[:$set]
                elem.merge(toupdate[:$set]) 
              elsif toupdate[:$inc]
                toupdate[:$inc].map { |k,v|
                  elem[k] ||= 0
                  elem[k] += v
                }
                elem
              else
                elem
              end
            else
              toupdate
            end
            elems << newelem
            newelem
          }
          set(@keyname, elems)
        end
      end

      def count(tofind = {})
        find(tofind).count
      end

      def find(tofind = {}, modifiers = {})
        elems = get(@keyname) || []

        if tofind.is_a?(BSON::ObjectId)
          return elems.select {|elem| elem[:_id] == tofind.to_s}.first
        end
        tofind.each_pair { |k,v| tofind[k] = v.to_s if v.is_a?(BSON::ObjectId) }
        elems.reject! { |elem| (elem.to_a - tofind.to_a).any? { |k,_|
            tofind.has_key?(k)
          }
        }
        if modifiers.keys.any? { |k| [:fields, :sort].include?(k) }
          if modifiers[:fields]
            raise "Not implemented yet in Rack::OAuth2::Membase"
          end
          if modifiers[:sort]
            puts "Not fully implemented yet in Rack::OAuth2::Membase"
          end
        end
        elems
      end

      def find_one(tofind, opts = {})
        elems = get(@keyname) || []

        if tofind.is_a?(BSON::ObjectId)
          return elems.select {|elem| elem[:_id] == tofind.to_s}.first
        end
        tofind.each_pair { |k,v| tofind[k] = v.to_s if v.is_a?(BSON::ObjectId) }
        elems.reject { |elem| (elem.to_a - tofind.to_a).any? { |k,_|
            tofind.has_key?(k)
          }
        }.first
      end

      def drop
        delete @keyname
      end

      def remove(opts = {})
        opts.each_pair { |k,v| opts[k] = v.to_s if v.is_a?(BSON::ObjectId) }
        lock do
          elems = get(@keyname) || []
          todel = elems.select { |elem| (elem.to_a - opts.to_a).any? { |k,v|
              opts.has_key?(k) && opts[k] == v
            }
          }
          elems.delete todel
          set(@keyname, elems)
        end
      end

      def collection(keyname)
        @keyname = keyname
        self
      end

      def [](keyname)
        collection(keyname)
      end

      def create_index
      end

      private
      def lock
        keylock = "oauth_lock_#{@keyname}"
        if get(keylock).nil? == false
          sleep 0.3
        end
        set(keylock, true)
        yield
        delete keylock
      end

    end
  end
end
