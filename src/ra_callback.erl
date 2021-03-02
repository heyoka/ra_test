%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Jun 2020 14:25
%%%-------------------------------------------------------------------
-module(ra_callback).
-behaviour(ra_machine).
-author("heyoka").


-define(CLUSTER_NAME, ra_faxe).
%% API
-export([init/1, apply/3, state_enter/2]).
-export([start/0, restart/0, started/3, stopped/3, get_pid/1, join/0, join/1, get_flows/1, get_procs/1, rejoin/1]).

-record(state, {
  ring,
  procs = #{}
}).


join() ->
  join(ra1@ubuntu).
join(To) ->
  pong = net_adm:ping(To),
  ra:add_member({?CLUSTER_NAME, To}, {?CLUSTER_NAME, node()}),
  ra:start_server(?CLUSTER_NAME, {?CLUSTER_NAME, node()}, {module, ?MODULE, #{}}, []).

rejoin(To) ->
  pong = net_adm:ping(To),
  restart().

restart() ->
  ra:restart_server({?CLUSTER_NAME, node()}).

started(Key, Node, Pid) ->
  ServerId = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  ra:process_command(ServerId, {started, Key, Node, Pid}).

stopped( Key, Node, Pid) ->
  ServerId = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  ra:process_command(ServerId, {stopped, Key, Node, Pid}).


get_procs(Key) ->
  Query =
  fun(#state{procs = Procs}) ->
    case maps:is_key(Key, Procs) of
      true -> {ok, maps:get(Key, Procs)};
      false -> {error, key_not_found}
    end
  end,
  {ok, Result, _} = ra:leader_query(?CLUSTER_NAME, Query),
  Result.


get_pid(Key) ->
  Leader = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  lager:notice("Leader from leaderboard: ~p",[Leader]),
  ra:process_command(Leader, {get_pid, Key}).

get_flows(Node) ->
  Leader = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  ra:process_command(Leader, {get_flows, Node}).

start( ) ->
  %% the initial cluster members
%%  [net_adm:ping(N) || N <- [ra1@alex, ra2@alex, ra3@alex, ra4@ubuntu, ra5@ubuntu]],
%%  Nodes = [node()|nodes()],
%%  Members = [{?CLUSTER_NAME, N} || N <- Nodes],
  Members = [{?CLUSTER_NAME, node()}],
  %% an arbitrary cluster name
%%  ClusterName = <<"ra_faxe">>,
  %% the config passed to `init/1`, must be a `map`
  Config = #{},
  %% the machine configuration
  Machine = {module, ?MODULE, Config},
  %% start a cluster instance running the `ClusterName` machine
  ra:start_cluster(?CLUSTER_NAME, Machine, Members).


init(_Config) ->
  lager:info("ra_callback init"),
  Nodes = [node()|nodes()],
  HRNodes = hash_ring:list_to_nodes(Nodes),
  Ring = hash_ring:make(HRNodes, [{module, hash_ring_dynamic}, {virtual_node_count, 64}]),
  ets:insert(ra_ring, {ring, Ring}),
  #state{ring = Ring}.


apply(_Meta, {started, Key, Node, Pid}, State = #state{procs = Flows}) ->
  lager:notice("[~p] STARTED graph ~p on node ~p" ,[?MODULE, Key, Node]),
  NewFlows = add_process(Key, Pid, Flows),
  lager:notice("New Flows: ~p",[NewFlows]),
  {State#state{procs = NewFlows}, ok};
apply(_Meta, {stopped, Key, Node, Pid}, State = #state{procs = Flows}) ->
  lager:notice("[~p] STOPPED graph ~p on node ~p" ,[?MODULE, Key, Node]),
  NewFlows = remove_process(Key, Pid, Flows),
  lager:notice("New FLows: ~p",[NewFlows]),
  {State#state{procs = NewFlows}, ok};
apply(Meta, {get_pid, Key}, State = #state{procs = Flows}) ->
  lager:notice("Metadata: ~p~n",[Meta]),
  lager:notice("get_pid, ~p : ~p",[Key, maps:get(Key, Flows, undefined)]),
  {State, maps:get(Key, Flows, undefined)};
apply(_Meta, {get_flows, Node}, State = #state{procs = Flows}) ->
  F = fun(_K, Pid) -> node(Pid) == Node end,
  Res = maps:filter(F, Flows),
  {State, Res};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

apply(_Meta, {nodeup, Node}, State=#state{ring = Ring, procs = Flows}) ->
%%  lager:warning("hash_ring-nodes: ~p",[hash_ring:get_nodes(Ring)]),
    case maps:is_key(Node, hash_ring:get_nodes(Ring)) of
      true ->
        lager:warning("nodeup but already exists in  ring ! ~p",[Node]),
        {State, ok};% do nothing
      false ->
        lager:notice("nodeup:add_node ~p",[hash_ring_node:make(Node)]),
        NewRing = hash_ring:add_node(hash_ring_node:make(Node), Ring),
        Effects = [
          {mod_call, ets, insert, [ra_ring, {ring, NewRing}]},
          {send_msg, graph_handler, {check_handoff, nodeup, Node, Flows}},
          {monitor, node, Node}],
        {State#state{ring = NewRing}, ok, Effects}
    end;
apply(_Meta, {nodedown, Node}, State=#state{ring = Ring, procs = Flows}) ->
  NewRing = hash_ring:remove_node(Node, Ring),
  lager:warning("nodedown: ~p",[Node]),
  F = fun(K, KeyList, Acc) ->
    Entries = [Pid || Pid <- KeyList, node(Pid) == Node],
    case Entries of
      [] -> Acc;
      _ -> Acc#{K => Entries}
    end
      end,
  NodeKeys = maps:fold(F, #{}, Flows),
  Effects = [
    {mod_call, ets, insert, [ra_ring, {ring, NewRing}]},
    {send_msg, graph_handler, {check_handoff, nodedown, Node, NodeKeys}}],
  {State#state{ring = NewRing}, ok, Effects}.

state_enter(leader, _State=#state{}) ->
  lager:alert("i am leader now: ~p",[node()]),
  %% re-request monitors for all nodes
  lager:warning("[~p] monitoring these nodes: ~p", [?MODULE, nodes()]),
  [{monitor, node, N} || N <- nodes()];%++[{call_mod, graph_handler, ring_changed, [Ring]}];
state_enter(NewState, _) ->
  lager:notice("entered state: ~p for: ~p", [NewState, node()]),
  [].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_process(Key, Pid, Processes) when is_map(Processes) ->
  case maps:get(Key, Processes, nil) of
    nil -> Processes#{Key => [Pid]};
    KeyList when is_list(KeyList) ->
      %% check if this pid is already present in the KeyList entry
      case lists:any(fun(P) -> P == Pid end, KeyList) of
        true ->
          %% we already have this pid with this key !
          Processes;
        false ->
          Processes#{Key => [Pid | KeyList]}
      end
  end.

remove_process(Key, Pid, Processes) when is_map(Processes) ->
  case maps:get(Key, Processes, nil) of
    nil -> Processes;
    KeyList ->
      case KeyList of
        [] ->
          lager:notice("KeyMap after removal: ~p",[maps:without([Key], Processes)]),
          maps:without([Key], Processes);
        [Pid] ->
          lager:notice("KeyMap after removal: ~p",[maps:without([Key], Processes)]),
          maps:without([Key], Processes);
        _L -> NewKeyList = lists:filter(fun(P) -> P /= Pid end, KeyList),
          lager:notice("KeyList after removal: ~p",[NewKeyList]),
          Processes#{Key => NewKeyList}
      end
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%% Tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
add_1_test() ->
  Map = #{key1 => [1]},
  Expected = Map,
  ?assertEqual(Expected, add_process(key1, 1, Map)).
add_2_test() ->
  Map = #{key1 => [1]},
  Expected = #{key1 => [1], key2 => [2]},
  ?assertEqual(Expected, add_process(key2, 2, Map)).
add_3_test() ->
  Map = #{key1 => [1]},
  Expected = #{key1 => [2, 1]},
  ?assertEqual(Expected, add_process(key1, 2, Map)).

remove_1_test() ->
  Map = #{key1 => [1]},
  Expected = #{},
  ?assertEqual(Expected, remove_process(key1, 1, Map)).

remove_2_test() ->
  Map = #{key1 => [1]},
  Expected = Map,
  ?assertEqual(Expected, remove_process(key3, 4, Map)).

remove_3_test() ->
  Map = #{key1 => [1, 2]},
  Expected = #{key1 => [2]},
  ?assertEqual(Expected, remove_process(key1, 1, Map)).

remove_4_test() ->
  Map = #{},
  Expected = Map,
  ?assertEqual(Expected, remove_process(key1, 1, Map)).

remove_5_test() ->
  Map = #{key1 => [1, 2]},
  Expected = #{},
  ?assertEqual(Expected, remove_process(key1, 1, remove_process(key1, 2, Map))).
-endif.