%%%-------------------------------------------------------------------
%% @doc ra_test top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(ra_test_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Ps = [
        {graph_handler,
            {graph_handler, start_link, []},
            permanent, 5000, worker, []},
        {handoff_handler,
            {handoff_handler, start_link, []},
            permanent, 5000, worker, []}
    ],
    {ok, {{one_for_one, 5, 20}, Ps}}.

%%====================================================================
%% Internal functions
%%====================================================================
