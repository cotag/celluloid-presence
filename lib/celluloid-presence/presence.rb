require 'socket'

module Celluloid
	module Presence
		class ZkPresence
			include Celluloid
			include Celluloid::Notifications
			
			PREFIX  ||= '/coPresence'
			NODE_PREFIX ||= 'node-'
			
			
			def self.finalizer
				:finalize
			end
			
			
			def initialize(options = {})
				@env = options[:env] || 'production'
				@base_path = options[:service_name].nil? ? "#{PREFIX}/#{@env}/default" : "#{PREFIX}/#{@env}/#{options[:service_name]}"
				@base_path_sym = @base_path.to_sym
				@node_path = nil	# This is officially set in connect_setup
				@service_name = options[:service_name] || :default
				@nodes = []
		
				if options[:node_address].nil?
					use_ip_v4 = !options[:ip_v6]		# IP v6 or IP v4 if using the default node data
					@node_address = proc { ip_address(use_ip_v4) }
				else
					node_address = options[:node_address]
					@node_address = proc { node_address.is_a?(Proc) ? node_address.call : node_address }
				end
				@last_known_address = nil
		
				#
				# These callbacks will be executed on a seperate thread
				# Which is why we need to use notifications update state
				#
				zk.register(@base_path)
				subscribe("zk_event_#{@base_path}", :zk_callback)
				subscribe('zk_connected', :on_connected)
				subscribe('zk_connecting', :on_connecting)
				
				on_connected if zk.connected?
			end
			
			
			#
			# Provides the list of known nodes
			#
			attr_reader :nodes
			
		
			#
			# This informs us of changes to the list of base path's children
			#
			def zk_callback(event_name, event)
				#p "EVENT OCUURED #{event.event_name} #{event.path}"
				if event.node_child?	# We are only listening to child events so we only need to handle these
					begin
						@nodes = zk.children(@base_path, :watch => true)    # re-set watch
						update_node_information
					rescue ZK::Exceptions::NoNode		# Technically this shouldn't happen unless someone deleted our base path
						on_connected if zk.connected?
					end
				end
			end
			
			#
			# This informs us of a new connection being established with zookeeper
			#
			def on_connected(event = nil)
				address = @node_address.call
				if @node_path.nil? or not zk.exists?(@node_path)	# Create presence node as it doesn't exist
					#p 'node re-created'
					@last_known_address = address
					connect_setup
				elsif @last_known_address != address		# Recreate existing presence node as our IP changed
					#p 'node ip_changed, recreating'
					zk.async.ensure(:delete, @node_path)
					@last_known_address = address
					connect_setup
				else												# Else our node presence information is accurate, lets get the latest list
					@nodes = zk.children(@base_path, :watch => true)
				end
				
				update_node_information								# inform listeners of a node list update
			end
		
			#
			# This informs us that we've been disconnected from zookeeper
			#
			def on_connecting event
				publish("#{@service_name}_nodes_stale")
			end
		
			#
			# Returns the value of a node
			#
			def get(node_id)
				result, _ = zk.get("#{@base_path}/#{node_id}")
				result
			rescue ZK::Exceptions::NoNode
			end
			
			#
			# Returns the name of this node
			#
			def name
				@node_path.split('/')[-1]
			end
		
		
			private
		
		
			#
			# Shortcut to the zookeeper service
			#
			def zk
				Actor[:zk_service]
			end
			
			#
			# creates the presence node
			#
			def connect_setup
				zk.mkdir_p @base_path
				@node_path = zk.create "#{@base_path}/#{NODE_PREFIX}", @last_known_address, :sequence => true, :ephemeral => true
				@nodes = zk.children @base_path, :watch => true
			end
		
			#
			# Pushes an updated node list to subscribers
			#
			def update_node_information
				publish("#{@service_name}_nodes", @nodes)
			end
			
			#
			# Grabs a valid IP address
			#
			def ip_address(ip_v4)
				ip = if ip_v4
					Socket.ip_address_list.detect {|a| a.ipv4? && !a.ipv4_loopback?}
				else
					Socket.ip_address_list.detect {|a| !a.ipv4? && !(a.ipv6_linklocal? || a.ipv6_loopback?)}
				end
				return ip.ip_address unless ip.nil?
				nil
			end
			
			#
			# On termination make sure zookeeper is updated
			#
			def finalize
				zk.async.unregister(@base_path)
				zk.async.ensure(:delete, @node_path) unless @node_path.nil?
			end
		end
	end
end
