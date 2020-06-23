%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Jun 2020 14:25
%%%-------------------------------------------------------------------
-module(ra_kv).
-behaviour(ra_machine).
-author("heyoka").


-define(CLUSTER_NAME, ra_faxe).
%% API
-export([init/1, apply/3, state_enter/2]).
-export([start/0, restart/0, started/3, stopped/3, get_pid/1, join/0, join/1, get_flows/1]).

-record(state, {
  ring,
  flows = #{}
}).


join() ->
  join(ra1@ubuntu).
join(To) ->
  ra:add_member({?CLUSTER_NAME, node()}, {?CLUSTER_NAME, To}),
  ra:start_server(?CLUSTER_NAME, {?CLUSTER_NAME, node()}, {module, ?MODULE, #{}}, []).

restart() ->
%%  ra:start_server(bla),
  ra:restart_server({?CLUSTER_NAME, node()}).

started(Key, Node, Pid) ->
  ServerId = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  ra:process_command(ServerId, {started, Key, Node, Pid}).

stopped( Key, Node, Pid) ->
  ServerId = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  ra:process_command(ServerId, {stopped, Key, Node, Pid}).

get_pid(Key) ->
  Leader = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  lager:notice("Leader from leaderboard: ~p",[Leader]),
  ra:process_command(Leader, {get_pid, Key}).

get_flows(Node) ->
  Leader = ra_leaderboard:lookup_leader(?CLUSTER_NAME),
  ra:process_command(Leader, {get_flows, Node}).

start( ) ->
  %% the initial cluster members
  [net_adm:ping(N) || N <- [ra1@ubuntu, ra2@ubuntu, ra3@ubuntu, ra4@ubuntu, ra5@ubuntu]],
  Nodes = [node()|nodes()],
  Members = [{?CLUSTER_NAME, N} || N <- Nodes],
  %% an arbitrary cluster name
%%  ClusterName = <<"ra_faxe">>,
  %% the config passed to `init/1`, must be a `map`
  Config = #{},
  %% the machine configuration
  Machine = {module, ?MODULE, Config},
  %% start a cluster instance running the `ClusterName` machine
  ra:start_cluster(?CLUSTER_NAME, Machine, Members).


init(_Config) ->
  Nodes = [node()|nodes()],
  HRNodes = hash_ring:list_to_nodes(Nodes),
  Ring = hash_ring:make(HRNodes),
  graph_handler:ring_changed(Ring),
  #state{ring = Ring}.


apply(_Meta, {started, Key, Node, Pid}, State = #state{flows = Flows}) ->
  lager:notice("[~p] STARTED graph ~p on node ~p" ,[?MODULE, Key, Node]),
  NewFlows = Flows#{Key => Pid},
  lager:notice("New Flows: ~p",[NewFlows]),
  {State#state{flows = NewFlows}, ok};
apply(_Meta, {stopped, Key, Node, Pid}, State = #state{flows = Flows}) ->
  lager:notice("[~p] STOPPED graph ~p on node ~p" ,[?MODULE, Key, Node]),
  NewFlows = maps:without([Key], Flows),
  lager:notice("New FLows: ~p",[NewFlows]),
  {State#state{flows = NewFlows}, ok};
apply(Meta, {get_pid, Key}, State = #state{flows = Flows}) ->
  lager:notice("Metadata: ~p~n",[Meta]),
  lager:notice("get_pid, ~p : ~p",[Key, maps:get(Key, Flows, undefined)]),
  {State, maps:get(Key, Flows, undefined)};
apply(_Meta, {get_flows, Node}, State = #state{flows = Flows}) ->
  F = fun(_K, Pid) -> node(Pid) == Node end,
  Res = maps:filter(F, Flows),
  {State, Res};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

apply(_Meta, {nodeup, Node}, State=#state{ring = Ring, flows = Flows}) ->
%%  lager:warning("hash_ring-nodes: ~p",[hash_ring:get_nodes(Ring)]),
    case maps:is_key(Node, hash_ring:get_nodes(Ring)) of
      true ->
        lager:warning("nodeup but already exists in  ring ! ~p",[Node]),
        {State, ok};% do nothing
      false ->
        lager:notice("nodeup:add_node ~p",[hash_ring_node:make(Node)]),
        NewRing = hash_ring:add_node(hash_ring_node:make(Node), Ring),
        graph_handler:ring_changed(NewRing),
        Effects = [{send_msg, graph_handler, {check_handoff, nodeup, Node, Flows}}],
        {State#state{ring = NewRing}, ok, Effects}
    end;
apply(_Meta, {nodedown, Node}, State=#state{ring = Ring, flows = Flows}) ->
  NewRing = hash_ring:remove_node(Node, Ring),
  lager:warning("nodedown: ~p",[Node]),
  graph_handler:ring_changed(NewRing),
  F = fun(_K, Pid) -> node(Pid) == Node end,
  NodeKeys = maps:filter(F, Flows),
  Effects = [{send_msg, graph_handler, {check_handoff, nodedown, Node, NodeKeys}}], 
  {State#state{ring = NewRing}, ok, Effects}.

state_enter(leader, State=#state{ring = Ring}) ->
  lager:alert("i am leader now: ~p",[node()]),
  %% re-request monitors for all nodes
  lager:warning("[~p] monitoring these nodes: ~p", [?MODULE, nodes()]),
  [{monitor, node, N} || N <- nodes()];%++[{call_mod, graph_handler, ring_changed, [Ring]}];
state_enter(NewState, _) ->
  lager:notice("entered state: ~p for: ~p", [NewState, node()]),
  [].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

