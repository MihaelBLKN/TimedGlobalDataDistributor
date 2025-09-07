--!strict
--@EnumEnv
-- Services --
local RunService = game:GetService("RunService")
local HttpService = game:GetService("HttpService")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local MessagingService = game:GetService("MessagingService")

-- Constants --
local ELECTION_CHANNEL = "ElectionTGDD"
local ELECTION_CHANNEL_QUESTIONS = {
	ElectNew = "ElectNew",
	GetCurrentLeader = "GetCurrentLeader",
}

local ACTIVITY_CHANNEL = "ActivityTGDD"
local ACTIVITY_CHANNEL_ANSWERS = {
	Active = "Active",
	Inactive = "Inactive",
}

local METADATA_CHANNEL = "TGDD_Metadata"
local METADATA_PREFIX = {
	Request = "[MREQ]",
	Response = "[MRES]",
}

local DEBUG_MODE = true
local LEADER_ID = game.JobId
local LEADER_ID_MESSAGE_PREFIX = "[LEADER]"
local ASSIGN_LEADER_ID_MESSAGE_PREFIX = "[ALEADER]"

-- Imports --
local Signal = require(script.FastSignal)

-- Debug Helper --
local function debugPrint(...)
	if DEBUG_MODE then
		print("[TGDD DEBUG]", ...)
	end
end

-- Class --
local TimedGlobalDataDistributor = {}
TimedGlobalDataDistributor.__index = TimedGlobalDataDistributor

-- Types --
export type DataType = any | { any? } | { [any?]: any? }
export type DataMetadata = {
	CurrentBuiltData: DataType?,
	CurrentTimeProgress: number,
}

export type Callbacks = {
	DataBuilder: () -> DataType,
	OnDataBuilt: (newData: DataType) -> (),
}

export type ActivityResponseRecievedSignal = Signal.ScriptSignal<string>
export type TimedGlobalDataDistributor = typeof(TimedGlobalDataDistributor) & {
	_onActivityResponseRecievedSignal: ActivityResponseRecievedSignal,
	_connections: { RBXScriptConnection },
	_timePerDistribution: number,
	_electionInProgress: boolean,
	_currentTimeDistributionProgress: number,
	_callbacks: Callbacks,
	_currentBuiltData: DataType?,
	_leaderActivityCheckThread: thread?,
	_timeDistributionConnection: RBXScriptConnection?,
	_leader: string?,
	_lastLeader: string?,
	_lastKnownMetadata: DataMetadata?,
	_isClosing: boolean?,
	_lastDataHash: string?,
	_callbackExecuting: boolean?,
}

-- Class Functions --
--- Creates a new instance of TimedGlobalDataDistributor
--- @param timePerDistribution number The seconds per each data distribution cycle.
--- @param callbacks Callbacks The callbacks for data building and handling.
--- @return TimedGlobalDataDistributor
function TimedGlobalDataDistributor.new(timePerDistribution: number, callbacks: Callbacks): TimedGlobalDataDistributor
	local self: TimedGlobalDataDistributor = setmetatable({}, TimedGlobalDataDistributor) :: any

	debugPrint("Initializing TGDD with timePerDistribution:", timePerDistribution, "seconds")
	debugPrint("Our LEADER_ID:", LEADER_ID)

	self._timePerDistribution = timePerDistribution
	self._onActivityResponseRecievedSignal = Signal.new()
	self._connections = {}
	self._callbacks = callbacks
	self._currentTimeDistributionProgress = 0
	self._electionInProgress = false
	self._currentBuiltData = nil
	self._lastKnownMetadata = nil
	self._isClosing = false
	self._lastDataHash = nil
	self._callbackExecuting = false
	
	-- Activity Channel --
	debugPrint("Subscribing to ACTIVITY_CHANNEL:", ACTIVITY_CHANNEL)
	table.insert(self._connections, MessagingService:SubscribeAsync(ACTIVITY_CHANNEL, function(message)
		self:_onActivityChannelCallback(message)
	end))

	-- Election Channel --
	debugPrint("Subscribing to ELECTION_CHANNEL:", ELECTION_CHANNEL)
	table.insert(self._connections, MessagingService:SubscribeAsync(ELECTION_CHANNEL, function(message)
		self:_onElectionChannelCallback(message)
	end))

	-- Metadata Channel
	debugPrint("Subscribing to METADATA_CHANNEL:", METADATA_CHANNEL)
	table.insert(self._connections, MessagingService:SubscribeAsync(METADATA_CHANNEL, function(message)
		self:_onMetadataChannelCallback(message)
	end))

	-- Get current leader --
	debugPrint("Publishing GetCurrentLeader request")
	MessagingService:PublishAsync(ELECTION_CHANNEL, ELECTION_CHANNEL_QUESTIONS.GetCurrentLeader)

	-- Bind to close --
	game:BindToClose(function()
		self:_onGameClose()
	end)

	return self
end

--- Gets the current leader.
--- @return string?
function TimedGlobalDataDistributor.GetLeader(self: TimedGlobalDataDistributor): string?
	debugPrint("GetLeader called, current leader:", self._leader)
	return self._leader
end

--- Checks if the current leader is still active.
--- @yields
--- @return boolean
function TimedGlobalDataDistributor.IsLeaderActiveAsync(self: TimedGlobalDataDistributor): boolean
	debugPrint("Checking if leader is active:", self._leader)

	local packIntoTable = false
	local timeout = 12

	local activityResultCallbacks = self:_getSignalResultAsync(self._onActivityResponseRecievedSignal, packIntoTable, timeout)
	debugPrint("Publishing activity check to ACTIVITY_CHANNEL")
	task.defer(MessagingService.PublishAsync, MessagingService, ACTIVITY_CHANNEL)
	local activityResult = activityResultCallbacks.WaitUntilResult() :: string?
	local isActive = activityResult == ACTIVITY_CHANNEL_ANSWERS.Active
	debugPrint("Leader activity check result:", activityResult, "-> isActive:", isActive)

	return isActive
end

--- Starts the time distribution cycle.
--- @param startFrom number? The time to start the distribution from.
--- @yields
function TimedGlobalDataDistributor.StartTimeDistribution(self: TimedGlobalDataDistributor, startFrom: number?)
	if self._timeDistributionConnection then
		self._timeDistributionConnection:Disconnect()
		self._timeDistributionConnection = nil
	end

	self._currentTimeDistributionProgress = startFrom or 0

	local lastTime = os.clock()
	self._timeDistributionConnection = RunService.Heartbeat:Connect(function()
		if (os.clock() - lastTime) < 1 then
			return
		end

		self._currentTimeDistributionProgress += 1
		self:_handleTimedDataCycle()
		lastTime = os.clock()
	end)
end

--- Gets the current metadata of the holding data of the timed distributor.
--- @return DataMetadata
function TimedGlobalDataDistributor.GetCurrentDataMetadata(self: TimedGlobalDataDistributor): DataMetadata
	return {
		CurrentBuiltData = self._currentBuiltData,
		CurrentTimeProgress = self._currentTimeDistributionProgress,
	}
end

--- Stops the time distribution cycle.
function TimedGlobalDataDistributor.StopTimeDistribution(self: TimedGlobalDataDistributor)
	if self._timeDistributionConnection then
		self._timeDistributionConnection:Disconnect()
		self._timeDistributionConnection = nil	
	end

	self._currentTimeDistributionProgress = 0
end

--- Destroys the TimedGlobalDataDistributor instance. (cleanup)
function TimedGlobalDataDistributor.Destroy(self: TimedGlobalDataDistributor)
	debugPrint("Destroying TGDD instance, disconnecting", #self._connections, "connections")

	for _, connection in self._connections do
		connection:Disconnect()
	end	

	table.clear(self._connections)
	debugPrint("TGDD instance destroyed")
end

--- NEW: Executes callback with duplicate prevention
--- @param data DataType The data to pass to callback
--- @param source string The source of the callback execution
function TimedGlobalDataDistributor._executeOnDataBuiltCallback(self: TimedGlobalDataDistributor, data: DataType, source: string)
	if not self._callbacks.OnDataBuilt then
		return
	end

	if self._callbackExecuting then
		debugPrint("Callback already executing, skipping duplicate call from:", source)
		return
	end
	
	local dataHash = ""
	if data then
		local ok, encoded = pcall(function()
			return HttpService:JSONEncode(data)
		end)
		if ok then
			dataHash = encoded
		else
			dataHash = tostring(data) .. tostring(os.clock())
		end
	end
	
	--[[
	if self._lastDataHash == dataHash and dataHash ~= "" then
		debugPrint("Duplicate data detected, skipping callback execution from:", source)
		return
	end
	]]

	self._lastDataHash = dataHash
	self._callbackExecuting = true

	debugPrint("Executing OnDataBuilt callback from:", source)
	local callbackSuccess = pcall(self._callbacks.OnDataBuilt, data)

	if not callbackSuccess then
		debugPrint("OnDataBuilt callback failed from:", source)
	else
		debugPrint("OnDataBuilt callback executed successfully from:", source)
	end

	self._callbackExecuting = false
end

--- Handles whenever metadata is recieved to server.
--- @param message { Data: string } The message received.
function TimedGlobalDataDistributor._onMetadataChannelCallback(self: TimedGlobalDataDistributor, message: { Data: string })
	local data = message.Data
	if string.sub(data, 1, #METADATA_PREFIX.Request) == METADATA_PREFIX.Request then
		if self._leader == LEADER_ID and not self._isClosing then
			debugPrint("Received metadata REQUEST, broadcasting our metadata")
			self:_broadcastMetadata()
		else
			debugPrint("Received metadata REQUEST but we are not leader or closing; ignoring")
		end
		return
	end

	if string.sub(data, 1, #METADATA_PREFIX.Response) == METADATA_PREFIX.Response then
		local jsonPart = string.sub(data, #METADATA_PREFIX.Response + 1)
		local ok, payload = pcall(function()
			return HttpService:JSONDecode(jsonPart)
		end)
		if ok and payload and payload.metadata then
			debugPrint("Received metadata RESPONSE from", payload.from, "at", payload.at)

			if payload.from ~= LEADER_ID then
				self._lastKnownMetadata = payload.metadata :: DataMetadata

				if self._leader ~= LEADER_ID and payload.metadata.CurrentBuiltData then
					self._currentBuiltData = payload.metadata.CurrentBuiltData
					self:_executeOnDataBuiltCallback(payload.metadata.CurrentBuiltData, "metadata_response")
				end
			end
		else
			debugPrint("Failed to parse metadata RESPONSE payload")
		end
	end
end

--- Callback for the activity channel.
--- @param message { Data: string } The message received.
function TimedGlobalDataDistributor._onActivityChannelCallback(self: TimedGlobalDataDistributor, message: { Data: string })
	debugPrint("Activity channel message received:", message.Data)

	if self._leader == LEADER_ID and not message.Data then --> we are the leader
		debugPrint("We are the leader, responding with Active")
		MessagingService:PublishAsync(ACTIVITY_CHANNEL, ACTIVITY_CHANNEL_ANSWERS.Active)
		return
	end

	debugPrint("Firing activity response signal with data:", message.Data)
	self._onActivityResponseRecievedSignal:Fire(message.Data)
end

--- Callback for the election channel.
--- @param message { Data: string } The message received.
function TimedGlobalDataDistributor._onElectionChannelCallback(self: TimedGlobalDataDistributor, message: { Data: string })
	debugPrint("Election channel message received:", message.Data)

	if message.Data == ELECTION_CHANNEL_QUESTIONS.ElectNew then
		if self._electionInProgress then
			debugPrint("Election already in progress, ignoring ElectNew request")
			return
		end

		debugPrint("ElectNew request received, clearing current leader")

		if self._leader then
			self._lastLeader = self._leader
		end

		self._leader = nil
		self:StopTimeDistribution()

		task.spawn(function()
			self:_findEligibleServer()
		end)
	elseif message.Data == ELECTION_CHANNEL_QUESTIONS.GetCurrentLeader then
		debugPrint("GetCurrentLeader request received")
		if self._leader == LEADER_ID then
			debugPrint("We are leader, publishing our ID")
			MessagingService:PublishAsync(ELECTION_CHANNEL, ASSIGN_LEADER_ID_MESSAGE_PREFIX .. LEADER_ID)
		else
			if not self._leader and not self:IsLeaderActiveAsync() then
				debugPrint("No leader known, triggering election")
				MessagingService:PublishAsync(ELECTION_CHANNEL, ELECTION_CHANNEL_QUESTIONS.ElectNew)
			end
		end
	elseif string.find(message.Data, ASSIGN_LEADER_ID_MESSAGE_PREFIX) then
		local splitResult = string.split(message.Data, ASSIGN_LEADER_ID_MESSAGE_PREFIX)
		if #splitResult >= 2 and splitResult[2] then
			local newLeader = splitResult[2]
			debugPrint("Leader assignment received:", newLeader)

			if self._leader == newLeader then
				debugPrint("Leader assignment matches current leader, ignoring")
				return
			end

			local previous = self._leader
			self._leader = newLeader

			if newLeader == LEADER_ID then
				debugPrint("We were assigned as leader (ASSIGN).")
				self:_onBecameLeader()
			else
				if previous == LEADER_ID then
					debugPrint("We are no longer leader, stopping distribution")
					self:StopTimeDistribution()
				end
			end
		else
			debugPrint("Failed to parse ASSIGN_LEADER message:", message.Data)
		end
	elseif string.find(message.Data, LEADER_ID_MESSAGE_PREFIX) then
		local splitResult = string.split(message.Data, LEADER_ID_MESSAGE_PREFIX)
		if #splitResult >= 2 and splitResult[2] then
			local candidateId = splitResult[2]
			debugPrint("Leader candidate received:", candidateId)

			if self._leader == candidateId then
				debugPrint("Candidate matches current leader, ignoring")
				return
			end

			self._leader = candidateId

			if candidateId == LEADER_ID then
				debugPrint("We have been elected as leader!")
				self:_onBecameLeader()
			else
				debugPrint("Another server elected as leader:", candidateId)
				self:StopTimeDistribution()
			end
		else
			debugPrint("Failed to parse LEADER_ID message:", message.Data)
		end
	end
end

--- Callback for the game close event.
--- Attempts to elect an new server when current one closes, and if the current one is the leader.
function TimedGlobalDataDistributor._onGameClose(self: TimedGlobalDataDistributor)
	self._isClosing = true
	if self._leader == LEADER_ID then
		self:_broadcastMetadata()
		task.wait(0.25)
		MessagingService:PublishAsync(ELECTION_CHANNEL, ELECTION_CHANNEL_QUESTIONS.ElectNew)
	end
end

--- Finds the first eligible server to elect as new leader and returns its job id.
--- @yields
--- @return string The job id of the new leader.
function TimedGlobalDataDistributor._findEligibleServer(self: TimedGlobalDataDistributor): string
	if self._electionInProgress then
		debugPrint("Election already in progress, aborting")
		return self._leader or LEADER_ID
	end

	self._electionInProgress = true
	debugPrint("Starting leader election process")

	local electionResponseSignal = Signal.new()
	local electedLeader = nil
	local electionComplete = false

	local electionConnection
	electionConnection = MessagingService:SubscribeAsync(ELECTION_CHANNEL, function(message)
		local data = message.Data
		debugPrint("Election response received:", data)

		if string.find(data, LEADER_ID_MESSAGE_PREFIX) and not electionComplete then
			local splitResult = string.split(data, LEADER_ID_MESSAGE_PREFIX)
			if #splitResult >= 2 and splitResult[2] then
				local candidateId = splitResult[2]
				debugPrint("Candidate ID extracted:", candidateId)

				if candidateId and not electedLeader then
					debugPrint("Electing candidate:", candidateId)
					electedLeader = candidateId
					self._leader = candidateId
					electionComplete = true
					electionResponseSignal:Fire(candidateId)					
				end
			else
				debugPrint("Failed to extract candidate ID from:", data)
			end
		end
	end)

	debugPrint("Publishing ElectNew message")
	local success = pcall(function()
		MessagingService:PublishAsync(ELECTION_CHANNEL, ELECTION_CHANNEL_QUESTIONS.ElectNew)
	end)

	if not success then
		debugPrint("Failed to publish election request")
		electionConnection:Disconnect()
		self._electionInProgress = false
		error("Failed to publish election request")
	end

	task.wait(0.2)

	local randomDelay = math.random() * 0.3 + 0.1
	debugPrint("Scheduling candidate announcement with delay:", randomDelay)
	task.spawn(function()
		task.wait(randomDelay)

		if self._lastLeader == LEADER_ID then
			debugPrint("We are the last leader, excluding ourselves from candidacy")
			return
		end

		if not electionComplete and not self._electionInProgress then
			debugPrint("Election completed while waiting, not announcing candidacy")
			return
		end

		if not electionComplete then
			local candidateMessage = LEADER_ID_MESSAGE_PREFIX .. LEADER_ID
			debugPrint("Announcing candidacy:", candidateMessage)
			pcall(function()
				MessagingService:PublishAsync(ELECTION_CHANNEL, candidateMessage)
			end)
		else
			debugPrint("Election already complete, not announcing candidacy")
		end
	end)

	local packIntoTable = false
	local timeout = 5
	local electionCallbacks = self:_getSignalResultAsync(electionResponseSignal, packIntoTable, timeout)

	debugPrint("Waiting for election result...")
	local result = electionCallbacks.WaitUntilResult()

	electionCallbacks.CleanupEarly()
	electionConnection:Disconnect()
	electionResponseSignal:Destroy()
	self._electionInProgress = false

	if not result then
		debugPrint("Election timeout, electing ourselves as leader")
		self._leader = LEADER_ID
		self:_onBecameLeader()
		return LEADER_ID
	end

	if self._leader == LEADER_ID then
		self:_onBecameLeader()
	end

	debugPrint("Election completed, final leader:", result)
	return result :: string
end

--- Handles the timed data cycle.
function TimedGlobalDataDistributor._handleTimedDataCycle(self: TimedGlobalDataDistributor)
	if self._currentTimeDistributionProgress >= self._timePerDistribution then
		self._currentTimeDistributionProgress = 0
		
		local success, data = pcall(function()
			return self._callbacks.DataBuilder()
		end)

		if not success then
			debugPrint("Failed to build data.")
		else
			self._currentBuiltData = data
			debugPrint("Data built successfully, broadcasting to other servers")
			self:_broadcastMetadata()
			self:_executeOnDataBuiltCallback(data, "timed_cycle")
		end
	end
end

--- Handles the callback from when the current server becomes a leader.
function TimedGlobalDataDistributor._onBecameLeader(self: TimedGlobalDataDistributor)
	debugPrint("Became leader, stopping current distribution and awaiting handoff")
	self:StopTimeDistribution()
	self:_awaitHandoffAndStart()
end

--- Handles the callback for whenever requesting metadata or cold-starting from old leader to properly build and start from where left off.
function TimedGlobalDataDistributor._awaitHandoffAndStart(self: TimedGlobalDataDistributor)
	self._lastKnownMetadata = nil

	local tempCapture = Signal.new()
	local tempConn = MessagingService:SubscribeAsync(METADATA_CHANNEL, function(message)
		if string.sub(message.Data, 1, #METADATA_PREFIX.Response) == METADATA_PREFIX.Response then
			local jsonPart = string.sub(message.Data, #METADATA_PREFIX.Response + 1)
			local ok, payload = pcall(function()
				return HttpService:JSONDecode(jsonPart)
			end)
			if ok and payload and payload.metadata then
				tempCapture:Fire(payload)
			end
		end
	end)

	debugPrint("Requesting metadata handoff from cluster")
	pcall(MessagingService.PublishAsync, MessagingService, METADATA_CHANNEL, METADATA_PREFIX.Request .. (self._lastLeader or ""))

	local callbacks = self:_getSignalResultAsync(tempCapture, false, 3)
	local resp = callbacks.WaitUntilResult()
	callbacks.CleanupEarly()
	tempConn:Disconnect()
	tempCapture:Destroy()

	if resp and resp.metadata then
		local md: DataMetadata = resp.metadata
		debugPrint("Handoff received, resuming at progress:", md.CurrentTimeProgress)
		self._currentBuiltData = md.CurrentBuiltData
		self._lastKnownMetadata = md

		if self._currentBuiltData then
			self:_executeOnDataBuiltCallback(self._currentBuiltData, "handoff_received")
		end

		self:StartTimeDistribution(math.max(0, md.CurrentTimeProgress or 0))
	else
		debugPrint("No handoff received; performing cold start")
		local built: DataType? = nil
		local ok, data = pcall(function()
			return self._callbacks.DataBuilder()
		end)

		if ok then
			built = data
			self._currentBuiltData = built
			self:_executeOnDataBuiltCallback(built, "cold_start")
		else
			debugPrint("Cold start DataBuilder failed; continuing with nil data")
		end

		self:StartTimeDistribution(0)
	end
end

--- Broadcasts current timed metadata with extra metadata to other servers.
function TimedGlobalDataDistributor._broadcastMetadata(self: TimedGlobalDataDistributor)
	local metadata = self:GetCurrentDataMetadata()
	local payload = {
		from = LEADER_ID,
		at = os.time(),
		metadata = metadata,
	}

	local ok, encoded = pcall(function()
		return HttpService:JSONEncode(payload)
	end)

	if ok then
		debugPrint("Broadcasting metadata handoff")
		pcall(MessagingService.PublishAsync, MessagingService, METADATA_CHANNEL, METADATA_PREFIX.Response .. encoded)
	else
		debugPrint("Failed to encode metadata payload for broadcast")
	end
end

--- Returns the result of a signal.
--- @param targetSignal Signal.ScriptSignal<any> The signal to get the result from.
--- @param pack boolean? If should pack tuple into table, if you have more than 1 argument.
--- @param timeOut number? The amount of seconds needed to pass to timeout and return nil.
--- @yields
--- @return any? | { [any?]: any? } | { any? } The result of the signal.
function TimedGlobalDataDistributor._getSignalResultAsync(
	self: TimedGlobalDataDistributor, 
	targetSignal: Signal.ScriptSignal<any>, 
	pack: boolean?, 
	timeOut: number?
): {
	WaitUntilResult: () -> any? | { [any?]: any? } | { any? },
	CleanupEarly: () -> ()	
	}
	debugPrint("Setting up signal result async with timeout:", timeOut or "none")

	local cleanedUp = false
	local result
	local signalConnection = targetSignal:Connect(function(...)
		local innerResult = ...
		result = pack and table.pack(...) or innerResult
		debugPrint("Signal fired with result:", result)
	end)

	local function CleanUp()
		if not cleanedUp then
			debugPrint("Cleaning up signal connection")
			cleanedUp = true

			if signalConnection and signalConnection.Connected then
				signalConnection:Disconnect()
				signalConnection = nil :: any
			end
		end
	end

	return {
		WaitUntilResult = function()
			debugPrint("Starting to wait for signal result...")
			local startTime = os.clock()

			repeat
				if cleanedUp then
					debugPrint("Signal cleanup detected, breaking wait loop")
					break
				end

				local elapsedTime = os.clock() - startTime
				if timeOut and elapsedTime >= timeOut then
					debugPrint("Signal wait timed out after", elapsedTime, "seconds")
					CleanUp()
					return nil
				end

				task.wait(0.1)
			until result or cleanedUp

			debugPrint("Signal wait completed with result:", result)
			return result
		end,

		CleanupEarly = CleanUp,
	}
end

-- End --
return TimedGlobalDataDistributor :: TimedGlobalDataDistributor
