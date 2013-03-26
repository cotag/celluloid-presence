require 'zk'

module Celluloid
	module Presence
		class ZkService
			include Celluloid
			include Celluloid::Notifications
			
			DEFAULT_PORT ||= 2181
			@@zk ||= nil				# The single zookeeper instance
			@@registry ||= {}			# Paths we are interested in receiving events from
			@@registry_count ||= {}		# Listener counts (for unregistering)
			@@ensure ||= []				# Tasks we want to ensure are completed (even when disconnected)
			@@connected ||= false		# We'll maintain our own connected state
			
			
			#
			# Optional init function to supervise this actor
			#
			def self.init(options)
				ZkService.supervise_as :zk_service, options
			end
			
			# Create a single connection to Zookeeper that persists through Actor crashes
			#
			# servers: a list of Zookeeper servers to connect to. Each server in the
			#          list has a host/port configuration
			def initialize(options)
				@@zk ||= begin
					Fanout.supervise_as :notifications_fanout if Actor[:notifications_fanout].nil?
		
					# Let them specify a single server instead of many
					servers = options[:server].nil? ? options[:servers] : [options[:server]]
					raise "no Zookeeper servers given" unless servers
		
					# Add the default Zookeeper port unless specified
					servers.map! do |server|
						if server[/:\d+$/]
							server
						else
							"#{server}:#{DEFAULT_PORT}"
						end
					end
		
					ZK.new(*servers) do |zk|
						zk.on_connecting &proc { Actor[:zk_service].async.on_connecting }
						zk.on_connected  &proc { Actor[:zk_service].async.on_connected }
					end
				end.tap do |zk|
					@@connected = zk.connected?
				end
			end
			
			#
			# Called once a connection is made to the zookeeper cluster
			# Runs any impending ensures before informing any subscribed actors
			#
			def on_connected
				@@connected = true
				while not @@ensure.empty?
					func = @@ensure.shift
					ensured func[0], *func[1]	# func[0] == function name, func[1] == arguments
				end
				publish('zk_connected')
			end
			
			#
			# Called on disconnect from zookeeper
			# Might be that the zookeeper node crashed or zookeeper is down, may not effect any other connections
			#
			def on_connecting
				@@connected = false
				publish('zk_connecting')
			end
		
			#
			# Publishes any zookeeper events that have been registered
			#
			def event_callback(path, event)
				publish("zk_event_#{path}", event)
			end
		
			#
			# Uses a closure to cleanly pull the zookeeper event back into the protection of celluloid
			#
			def register(path)
				path = path.to_sym
				if @@registry[path].nil?
					callback = proc { |event| Actor[:zk_service].async.event_callback(path, event) }
					@@registry[path] = @@zk.register(path.to_s, &callback)
				end
				@@registry_count[path] = (@@registry_count[path] || 0) + 1
			end
			
			#
			# unsubscribes from zookeeper events once there are no more listeners
			#
			def unregister(path)
				path = path.to_sym
				if @@registry[path]
					@@registry_count[path] -= 1
					if @@registry_count[path] <= 0
						sub = @@registry.delete(path)
						sub.unsubscribe
						@@registry_count.delete(path)
					end
				end
			end
		
			#
			# Our abstracted connected status to avoid race conditions
			#
			def connected?
				@@connected
			end
		
			#
			# Ensures important requests are followed through in the case of disconnect / reconnect
			# Ignores request if the node does not exist
			# USE SPARINGLY
			#
			def ensure(func, *args)
				if connected?
					ensured(func, *args)
				else
					@@ensure.push [func, args]
				end
			end
		
			
			#
			# Proxy the following functions
			#
			# Automatically creates a callable function for each command
			#	http://blog.jayfields.com/2007/10/ruby-defining-class-methods.html
			#	http://blog.jayfields.com/2008/02/ruby-dynamically-define-method.html
			#
			SAFE_OPS ||= [:get, :children, :create, :mkdir_p, :delete, :stat, :exists?, :set, :rm_rf]
			SAFE_OPS.each do |func|
				define_method func do |*args|
					begin
						@@zk.send func, *args if connected?
					rescue Zookeeper::Exceptions::NotConnected
					end
				end
			end
		
		
			private
		
			
			def ensured(func, *args)
				@@zk.send func, *args
			rescue Zookeeper::Exceptions::NotConnected
				@@ensure << [func, args]
			rescue ZK::Exceptions::NoNode
			end
		end
	end
end
