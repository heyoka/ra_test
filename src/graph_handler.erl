%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(graph_handler).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-export([start_flow/2, stop_flow/1, ring_changed/1]).

-define(SERVER, ?MODULE).

-record(cluster_mode, {
  singleton = true,
  replicas = 0
}).

-record(graph_handler_state, {
  ra_leader,
  flows,
  ring
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_flow(Key, Opts) ->
  gen_server:call(?SERVER, {start_flow, Key, Opts}).

stop_flow(Key) ->
  gen_server:call(?SERVER, {stop_flow, Key}).

ring_changed(NewRing) ->
  ?SERVER ! {ring_changed, NewRing}.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #graph_handler_state{}}.

handle_call({start_local, Key, Opts}, _From, State = #graph_handler_state{}) ->
  Res  = faxe_start_task(Key, Opts),
  lager:notice("[~p] started task locally as : ~p : ~p",[?MODULE, {Key, node()}, Res]),
  {reply, Res, State};
handle_call({start_flow, Key, Opts}, _From, State = #graph_handler_state{ring = Ring, ra_leader = _Leader}) ->
  %% start Opts number of flows, if not already started
  lager:notice("[~p] START_FLOW: on node: ~p",[?MODULE, preflist(Key, Ring, 1)]),
%%  Node = find_node(Key, Ring),
  Res = start_local(Key, Ring, 1),
  {reply, Res, State};
handle_call({stop_flow, Key}, _From, State = #graph_handler_state{ra_leader = _Leader}) ->
  %% check for pids of key and stop all of them
  case ra_kv:get_pid(Key) of
    {ok, undefined, _Leader} -> ok;
    {ok, Pid, _Leader} when is_pid(Pid) ->
      faxe_stop_task(Pid),
      ra_kv:stopped(Key, node(Pid), Pid)
  end,
  {reply, ok, State}.

handle_cast(_Request, State = #graph_handler_state{}) ->
  {noreply, State}.

handle_info({ring_changed, NewRing}, State = #graph_handler_state{}) ->
  lager:notice("[~p] ring_changed!!, nodes: ~p", [?MODULE, hash_ring:get_nodes(NewRing)]),
  {noreply, State#graph_handler_state{ring = NewRing}};
handle_info({check_handoff, nodedown, Node, KeyMap}, State = #graph_handler_state{ring = Ring, ra_leader = _Leader}) ->
  %% somehow we need to find out if multi or not
  lager:warning("[~p] node_down: ~p, relocate tasks:~p",[?MODULE, Node, KeyMap]),
  %% check Tasks on node Node and relocate them to their new node
  F =
    fun(K, _Pid) ->
      NewNode = find_node(K, Ring),
      lager:notice("[~p] new node for handoff key: ~p : ~p",[?MODULE, K, NewNode]),
      {ok, NewPid} = start_local(K, Ring, #cluster_mode{}),
%%      {ok, NewPid} = gen_server:call({?SERVER, NewNode}, {start_local, K, []}),
      lager:alert("[~p] ~p is now on node ~p after nodedown.",[?MODULE, K, NewNode]),
%%      ra_kv:started(K, NewNode, NewPid),
      {K, NewNode, NewPid}
    end
  ,
  maps:map(F, KeyMap),
  {noreply, State#graph_handler_state{}};
handle_info({check_handoff, nodeup, Node, KeyMap}, State = #graph_handler_state{ring = Ring}) ->
  %%
  lager:warning("[~p] node_up: ~p, check_handoff:~p",[?MODULE, Node, KeyMap]),
  %% ring grows and we have to relocate some tasks
  F =
    fun(K, Pid) ->
      NewNode = find_node(K, Ring),
      case NewNode /= node(Pid) of
        true ->
          lager:notice("[~p] Key: ~p currently on node ~p New Node will be: ~p",[?MODULE, K, node(Pid), NewNode]),
          start_handoff(K, Pid, NewNode);
        false -> lager:warning("[~p] NewNode(~p) == Node(~p) in check_handoff nodeup",[?MODULE, NewNode, node(Pid)]),ok %% do nothing
      end,
%%      {ok, NewPid} = gen_server:call({?SERVER, NewNode}, {start_local, K, []}),
      {K, NewNode, Pid}
    end
  ,
  maps:map(F, KeyMap),

  {noreply, State#graph_handler_state{}};
handle_info(_, State = #graph_handler_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #graph_handler_state{}) ->
  ok.

code_change(_OldVsn, State = #graph_handler_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

find_node(Key, Ring) ->
  Res = hash_ring:find_node(Key, Ring),
  lager:notice("[~p] found node: ~p",[?MODULE, Res]),
  {ok, {hash_ring_node, Node, Node, _}} = Res,
  Node.

-spec preflist(Ring :: term(), Key :: term()) -> [{non_neg_integer(), atom()}].
preflist(Key, Ring) ->
  preflist(Ring, Key, length(nodes())+1).
preflist(Key, Ring, Num) ->
  PrefNodes = hash_ring:collect_nodes(Key, Num, Ring),
  [Node || {hash_ring_node, Node, Node, _Weight} <- PrefNodes].

start_local(Key, Ring, 1) ->
  do_start(Key, find_node(Key, Ring)).

do_start(Key, Node) ->
  Res =
    case node() == Node of
      true -> faxe_start_task(Key, []);
      false -> gen_server:call({?SERVER, Node}, {start_local, Key, []})
    end,
  case Res of
    {ok, Pid} -> ra_kv:started(Key, Node, Pid), {ok, Pid};
    E -> E
  end.

faxe_start_task(Key, _Opts) ->
  graph:start_link(Key).

faxe_stop_task(Pid) ->
  gen_server:stop(Pid).

start_handoff(Key, FromPid, ToNode) ->
%%  OldNode = node(FromPid),
  lager:notice("[~p] start handoff for ~p, ~p, ~p", [?MODULE, Key, FromPid, ToNode]),
  case do_start(Key, ToNode) of
    {ok, Pid} ->
      lager:alert("[~p] ~p is now on node ~p after handoff.",[?MODULE, Key, ToNode]),
      faxe_stop_task(FromPid);
      %ra_kv:stopped(Key, node(FromPid), FromPid);
    E -> E
  end.