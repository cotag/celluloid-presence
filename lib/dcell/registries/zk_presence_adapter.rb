require 'celluloid-presence'


module DCell
	module Registry
		class ZkPresenceAdapter
			def initialize(options)				
				Celluloid::Presence::ZkService.init options
				
				@node_registry = NodeRegistry.new(options)
				@global_registry = GlobalRegistry.new(:zk_service, options)
			end
			
			
			class NodeRegistry
				include Celluloid
				include Celluloid::Notifications
				
				def initialize()
					@mutex = Mutex.new
					@current_address = nil
					
					Celluloid::Presence::ZkPresence.supervise_as :dcell_presence, {
						:service_name => :dcell_presence,
						:node_address => proc { @mutex.synchronize { @current_address } },
						:env => options[:env] || 'production'
					}	
								
					@service_name = :dcell_presence
					@directory = {}	# node_name => address
					@mapping = {}	# zk_path => node_name
					
					subscribe("#{@service_name}_nodes", :node_update)
					
					@nodes = []
					update_directory(presence.nodes)
				end
				
				def get(node_id)
					@directory[node_id]
				end
				
				def nodes
					@directory.keys
				end
				
				def node_update(event, nodes)
					update_directory(nodes)
				end
				
				def set(node_id, addr)
					@mutex.synchronize {
						@current_address = Marshal.dump [node_id, addr]
					}
					presence.on_connected	# This causes an address update on zookeeper
				end
				
				
				private
				
				
				def update_directory(new_list)
					unsubscribe = @nodes - new_list		# TODO:: use sets here instead of arrays for speed
					subscribe = new_list - @nodes
					
					unsubscribe.each do |node|
						node_name = @mapping.delete(node)
						@directory.delete(node_name)
					end
					
					subscribe.each do |node|
						value = presence.get(node)
						if value.nil?
							@nodes.delete value
						else
							begin
								info = Marshal.load value
								@mapping[node] = info[0]
								@directory[info[0]] = info[1]
							rescue
								@nodes.delete value
							end
						end
					end
				end
				
				def presence
					Actor[@service_name]
				end
			end
			
			def clear_nodes;				end		# This is now self maintaining hence no op
			def get_node(node_id);			@node_registry.get(node_id) end
			def nodes;						@node_registry.nodes end
			def set_node(node_id, addr);	@node_registry.set(node_id, addr) end
			
			
			
			class GlobalRegistry
				PREFIX  = "/dcell_global"
				
				def initialize(service_name, options)
					options = options.inject({}) { |h,(k,v)| h[k.to_s] = v; h }
					
					@service_name = service_name
					@env = options['env'] || 'production'
					@base_path = "#{PREFIX}/#{@env}"
				end
				
				def get(key)
					value, _ = zk.get "#{@base_path}/#{key}"
					Marshal.load value
				rescue ZK::Exceptions::NoNode
				end
				
				# Set a global value
				def set(key, value)
					path = "#{@base_path}/#{key}"
					string = Marshal.dump value
				
					zk.set path, string
				rescue ZK::Exceptions::NoNode
					zk.create path, string
				end
				
				# The keys to all globals in the system
				def global_keys
					zk.children(@base_path)
				end
				
				def clear
					zk.rm_rf @base_path
					zk.mkdir_p @base_path
				end
				
				private
				
				def zk
					Celluloid::Actor[@service_name]
				end
			end
			
			
			def clear_globals;			@global_registry.clear end
			def get_global(key);		@global_registry.get(key) end
			def set_global(key, value);	@global_registry.set(key, value) end
			def global_keys;			@global_registry.global_keys end
		end
	end
end
