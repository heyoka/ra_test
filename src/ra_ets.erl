%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(ra_ets).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(ra_ets_state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
new_table(NameAtom, Type) ->
  ets:new(NameAtom, [Type, public, named_table, {read_concurrency,true},{heir, self(), NameAtom} ]),
  ok.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  ok = new_table(ra_ring, set),
  {ok, #ra_ets_state{}}.

handle_call(_Request, _From, State = #ra_ets_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #ra_ets_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #ra_ets_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #ra_ets_state{}) ->
  ok.

code_change(_OldVsn, State = #ra_ets_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
