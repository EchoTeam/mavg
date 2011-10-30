%%% Copyright (c) 2007, 2008, 2009 JackNyfe, Inc. <info@jacknyfe.net>.
%%% See the accompanying LICENSE file.

-module(jn_mavg).

%%
%% This module implements exponential moving average logic,
%% a useful data structure to store hits/second averaged over some time period.
%%
%% For a general description see:
%% http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
%%

-export([
	bump_mavg/2,
	getEventsPer/2,
	getEventsPer_nobump/2,
	getProperties/1,
	get_current/1,
	history/1,
	new_mavg/1,
	new_mavg/2
]).

% Time/Event moving average representation

-record(ecnt, {
	counter = 0,		% Number of events counter
	period_start = 0,	% Timestamp of period start
	history = [],		% Counters: list of tuples {PeriodStart,Count}
	archived_events = 0,	% Total number of seen and archived events.
	history_length = 3	% Max length of history list
    }).

-record(mavg, {
	period = 300,		% Smoothing window
	createts,		% Time of creation of this structure
	lastupdatets,		% Last update time stamp
	unprocessedEvents = 0,	% Number of events not counted in historicAvg
	historicAvg = 0.0,	% Number of events in this period (float)
	eventCounter = #ecnt{}	% Collect absolute number of events
    }).

%% Construct a moving average tracker with a specified period.
%% This is a shortcut which specifies default options.
%% @spec new_mavg(SmoothingWindow) -> record(mavg)
%% Type	SmoothingWindow = 30 | 300 | 86400 | int()
new_mavg(SmoothingWindow) -> new_mavg(SmoothingWindow, []).

%% New way of constructing moving average trackers.
%% @spec new_mavg(SmoothingWindow, [Option]) -> record(mavg)
%% Type		Option =
%%			  {start_time, int()}
%%			| {start_events, int()}
%%			| {history_length, int()}
new_mavg(SmoothingWindow, Options) when
	is_integer(SmoothingWindow), SmoothingWindow >= 10, is_list(Options) ->
	Time = proplists:get_value(start_time, Options, unixtime()),
	Events = proplists:get_value(start_events, Options, 0),
	HLength = proplists:get_value(history_length, Options,
                                  (#ecnt{})#ecnt.history_length),
	#mavg{period = SmoothingWindow, lastupdatets = Time, createts = Time,
          eventCounter = updateEventCounter(Events,
                                            #ecnt{history_length=HLength},
                                            Time, SmoothingWindow),
          unprocessedEvents = Events };

%% Old way of constructing moving average trackers.
%% Create a new mavg record with a specified smoothing period.
%% @spec new_mavg(int(), int()) -> record(mavg)

new_mavg(SmoothingWindow, Events) when
		is_integer(SmoothingWindow), SmoothingWindow >= 10,
		is_integer(Events), Events >= 0 ->
	new_mavg(SmoothingWindow, [{start_events, Events},
                               {start_time, unixtime()}]).

% Add some number of events into the time counter.
%% @spec bump_mavg(record(mavg), int()) -> record(mavg)
%% @spec bump_mavg(record(mavg), int(), Unixtime) -> record(mavg)

bump_mavg(MA, Events) -> bump_mavg(MA, Events, unixtime()).
bump_mavg(MA, Events, T) when
      is_record(MA, mavg),
      is_integer(Events), Events >= 0,
      is_integer(T) ->
    #mavg{ period = Period, lastupdatets = Updated,
           unprocessedEvents = HoldEvs, historicAvg = Average,
           eventCounter = Counter } = MA,
    UpdatedCounter = updateEventCounter(Events, Counter, T, Period),
    Elapsed = T - Updated,
    if
	% We lose precision if we incorporate each update
	% into the pool right away, therefore we collect events
	% and update them not earlier than once a second or so.
        Elapsed =:= 0 -> MA#mavg{unprocessedEvents = HoldEvs + Events,
                                 eventCounter = UpdatedCounter };
        Elapsed < (8 * Period), Elapsed > 0 ->
            %% Integrate HoldEvs, since they're for a single period
            HoldAvg = (Average - HoldEvs) * math:exp(-1/Period) + HoldEvs,
            %% Integrate zero-filled periods, of which there are (Elapsed-1)
            ZeroAvg = HoldAvg * math:exp((1-Elapsed)/Period),
            MA#mavg{unprocessedEvents = Events, historicAvg = ZeroAvg,
                    lastupdatets = T, eventCounter = UpdatedCounter };
        true ->
            MA#mavg{unprocessedEvents = Events, historicAvg = 0.0,
                    lastupdatets = T, eventCounter = UpdatedCounter }
    end.

updateEventCounter(Events, EventsCounter, NowTS, Period) ->
	EC = EventsCounter,
	#ecnt{ counter = C, period_start = PeriodStart,
		history_length = MaxHistLength } = EC,
	% Make it look like local timestamp, useful for day-breaking.
	PST_TS = NowTS - 3600 * 8,	% Pacific Standard Time, hard-coded
	% Figure out whether EC corresponds to a current period or not.
	CurrentPeriod = PST_TS div Period,
	if
		CurrentPeriod == PeriodStart -> EC#ecnt{counter = Events + C};
		PeriodStart == 0 -> EC#ecnt{counter = Events + C,
                                    period_start = CurrentPeriod };
		true ->
            UpdatedHistory = updateEventHistory(EC#ecnt.history,
                                                PeriodStart, C, MaxHistLength),
            PaddedHistory = padHistoryUntil(CurrentPeriod - 1, UpdatedHistory,
                                            MaxHistLength),
			EC#ecnt{counter = Events,
                    period_start = CurrentPeriod,
                    archived_events = EC#ecnt.archived_events + C,
                    history = PaddedHistory }
	end.

padHistoryUntil(LastPeriod, [{Period,_}|_] = History, MaxHistLen) ->
	Skipped = LastPeriod - Period,
	if
        Skipped =< 0 ->
            History;
        true ->
            PTSes = lists:seq(LastPeriod,
                              lists:max([Period + 1, LastPeriod - MaxHistLen]),
                              -1),
            SkippedEntries = [{PTS, 0} || PTS <- PTSes],
            lists:sublist(SkippedEntries ++ History, MaxHistLen)
	end;
padHistoryUntil(_LastPeriod, [], _MaxHistLen) -> [].

updateEventHistory([{OldPeriodStart,_}|_] = PrevHistory, PeriodStart, Events,
                   MaxHistLen) when PeriodStart - OldPeriodStart == 1 ->
	lists:sublist([{PeriodStart,Events} | PrevHistory], MaxHistLen);
updateEventHistory([{OldPeriodStart,_}|_] = PrevHistory, PeriodStart, Events,
                   MaxHistLen) ->
	Skipped = PeriodStart - OldPeriodStart - 1,
	EntriesForSkippedPeriod = if
                                  Skipped =< 0 -> [];
                                  true ->
                                      PeriodEnd =lists:max([OldPeriodStart + 1,
                                                            PeriodStart -
                                                                MaxHistLen]),
                                      PTSes = lists:seq(PeriodStart - 1,
                                                        PeriodEnd,
                                                        -1),
                                      [{PTS, 0} || PTS <- PTSes ]
                              end,
	lists:sublist(
      [{PeriodStart,Events} | EntriesForSkippedPeriod] ++ PrevHistory,
      MaxHistLen);
updateEventHistory([], _, 0, _) -> [];
updateEventHistory([], PeriodStart, Events, _) -> [{PeriodStart, Events}].

% Get number of events per given number of time (extrapolated).
%% @spec getEventsPer(record(mavg), int()) -> int()

getEventsPer(MA, SomePeriod) when
      is_record(MA, mavg),
      is_integer(SomePeriod), SomePeriod > 0 ->
	MA_Updated = bump_mavg(MA, 0),	% Make sure we're current
	#mavg{ historicAvg = Average } = MA_Updated,
	EventsPerPeriod = Average,
	round(EventsPerPeriod * SomePeriod).

getEventsPer_nobump(#mavg{historicAvg = Average} = MA, SomePeriod) when
      is_record(MA, mavg),
      is_integer(SomePeriod), SomePeriod > 0 ->
	round(Average * SomePeriod).

getProperties(MA) ->
	#mavg{period = P, createts = C, lastupdatets = L} = MA,
	{P,C,L}.

history(MA) ->
	MA_Updated = bump_mavg(MA, 0), % Make sure we're current
	#ecnt{counter = C, history = H,
          archived_events = A} = MA_Updated#mavg.eventCounter,
	{C, [B || {_A, B} <- H], A}.

get_current(MA) when is_record(MA, mavg) ->
	MA_Updated = bump_mavg(MA, 0),	% Make sure we're current
	MA_Updated#mavg.historicAvg.

% Time stamp of current time.
%% @spec unixtime() -> integer()
unixtime() -> unixtime(now()).

%% @spec unixtime(now()) -> integer()
unixtime({Mega, Secs, _Msecs}) -> Mega * 1000000 + Secs.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

jn_mavg_test() ->
	io:format("~p Testing START ~n", [?MODULE]),
	[] = updateEventHistory([], 1200, 0, 42),
	[{1200, 13}] = updateEventHistory([], 1200, 13, 42),
	[{1201, 13},{1200, 5}] = updateEventHistory([{1200, 5}], 1201, 13, 42),
	[{1201, 13},{1200, 5},foo] = updateEventHistory([{1200, 5},foo],
                                                    1201, 13, 42),
	[{1200, 1}] = padHistoryUntil(1200, [{1200, 1}], 42),
	[{1201, 0},{1200, 1}] = padHistoryUntil(1201, [{1200, 1}], 42),
	[{1202, 0},{1201,0},{1200, 1}] = padHistoryUntil(1202, [{1200, 1}], 42),
	[{1202, 0},{1201,0},{1200, 1}] = padHistoryUntil(1202, [{1200, 1}], 3),
	[{1202, 0},{1201,0}] = padHistoryUntil(1202, [{1200, 1}], 2),
	[{1202, 0}] = padHistoryUntil(1202, [{1200, 1}], 1),
	MA1 = new_mavg(300),
	MA2 = bump_mavg(MA1, 60),
	io:format("tc1: ~p~n", [MA1]),
	io:format("tc2: ~p, wait...~n", [MA2]),
	timer:sleep(1200),
	MA3 = MA2#mavg{historicAvg = 60},
	MA4 = bump_mavg(MA3, 20),
	timer:sleep(1200),
	Ep3 = getEventsPer(MA3, 60),
	Ep4 = getEventsPer(MA4, 60),
	io:format("tc3: ~p, epm ~p~n", [MA3, Ep3]),
	io:format("tc4: ~p, epm ~p~n", [MA4, Ep4]),
	if
		Ep3 < 3575; Ep3 > 3590 -> throw("Assertion failed Ep3");
		Ep4 < 3578; Ep4 > 3595 -> throw("Assertion failed Ep4");
		true -> true
	end,
	T = (unixtime() div 300) * 300 + 20,
	MA11 = new_mavg(300, [{start_time, T}]),
	MA5 = bump_mavg(MA11, 1, T),
	io:format("tc5: ~p~n", [MA5]),
	#mavg{eventCounter = #ecnt{counter = 1, archived_events = 0}} = MA5,
	MA6 = bump_mavg(MA5, 1, T + 10),
	io:format("tc6: ~p~n", [MA6]),
	#mavg{eventCounter = #ecnt{counter = 2, archived_events = 0}} = MA6,
	MA7 = bump_mavg(MA6, 1, T + 280),
	io:format("tc7: ~p~n", [MA7]),
	#mavg{eventCounter = #ecnt{counter = 1, archived_events = 2}} = MA7,
	MA8 = bump_mavg(MA7, 1, T + 600),
	io:format("tc8: ~p~n", [MA8]),
	#mavg{eventCounter = #ecnt{counter = 1, archived_events = 3}} = MA8,

	% History testing
	HMa1 = new_mavg(60, [{start_time, unixtime() - 1000},
                         {start_events, 1}, {history_length, 0}]),
	{_, H1, _} = history(HMa1),
	0 = length(H1),

	HMa2 = new_mavg(60, [{start_time, unixtime() - 1000},
                         {start_events, 1}, {history_length, 2}]),
	{_, H2, _} = history(HMa2),
	2 = length(H2),

	HMa10 = new_mavg(60, [{start_time, unixtime() - 1000},
                          {start_events, 1}, {history_length, 10}]),
	{_, H10, _} = history(HMa10),
	10 = length(H10),

	HMa20 = new_mavg(60, [{start_time, unixtime() - 1200},
                          {start_events, 1}, {history_length, 20}]),
	{_, H20, _} = history(HMa20),
	[1|_] = lists:reverse(H20),
	20 = length(H20),

	io:format("~p Testing STOP ~n", [?MODULE]).

-endif.
