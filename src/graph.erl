%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(graph).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(graph_state, {
  id
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(FlowId) ->
  gen_server:start_link(?MODULE, [FlowId], []).

init([FlowId]) ->
  lager:info("[~p] start flow with id: ~p", [?MODULE, FlowId]),
  {ok, #graph_state{id = FlowId}}.

handle_call(_Request, _From, State = #graph_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #graph_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #graph_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #graph_state{}) ->
  ok.

code_change(_OldVsn, State = #graph_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
