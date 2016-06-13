%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidotec_pb).

-include_lib("riak_pb/include/antidote_pb.hrl").


-export([start_transaction/3,
	 start_transaction/4,
         abort_transaction/2,
         abort_transaction/3,
         commit_transaction/2,
         commit_transaction/3,
         update_objects/3,
         update_objects/4,
         read_objects/3,
         read_objects/4,
	 get_objects/3,
	 get_objects/4,
	 get_log_operations/4,
	 get_log_operations/3]).

-define(TIMEOUT, 10000).

-spec start_transaction(Pid::term(), TimeStamp::term(), TxnProperties::term())
        -> {ok,{interactive,term()} | {static,{term(),term()}}} | {error, term()}.
start_transaction(Pid, TimeStamp, TxnProperties) ->
    start_transaction(Pid, TimeStamp, TxnProperties, proto_buf).

start_transaction(Pid, TimeStamp, TxnProperties, ReplyType) ->
    case is_static(TxnProperties) of
        true -> 
            {ok, {static, {TimeStamp, TxnProperties}}};
        false ->
            EncMsg = antidote_pb_codec:encode(start_transaction,
                                              {TimeStamp, TxnProperties, ReplyType}),
            Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
            case Result of
                {error, timeout} -> 
                    {error, timeout};
                _ ->
                    case antidote_pb_codec:decode_response(Result) of
                        {start_transaction, TxId} ->
                            {ok, {interactive, TxId}};
                        {error, Reason} ->
                            {error, Reason};
                        Other ->
                            {error, Other}
                    end
            end
    end.

-spec abort_transaction(Pid::term(), TxId::term()) -> ok.
abort_transaction(Pid, {interactive, TxId}) ->
    abort_transaction(Pid, {interactive, TxId}, proto_buf).

abort_transaction(Pid, {interactive, TxId}, ReplyType) ->
    EncMsg = antidote_pb_codec:encode(abort_transaction, {TxId, ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {opresponse, ok} -> ok;
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.

-spec commit_transaction(Pid::term(), TxId::{interactive,term()} | {static,term()}) ->
                                {ok, term()} | {error, term()}.
commit_transaction(Pid, {interactive, TxId}) ->
    commit_transaction(Pid, {interactive, TxId}, proto_buf);
commit_transaction(Pid, {static, TxId}) ->
    commit_transaction(Pid, {static, TxId}, proto_buf).


commit_transaction(Pid, {interactive, TxId}, ReplyType) ->
    EncMsg = antidote_pb_codec:encode(commit_transaction, {TxId,ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {commit_transaction, CommitTimeStamp} -> {ok, CommitTimeStamp};
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end;
commit_transaction(Pid, {static, _TxId}, _ReplyType) ->
    case antidotec_pb_socket:get_last_commit_time(Pid) of
        {ok, CommitTime} ->
             {ok, CommitTime}
    end.

%%-spec update_objects(Pid::term(), Updates::[{bound_object(), op(), op_parm()}], TxId::term()) -> ok | {error, term()}.
update_objects(Pid, Updates, {interactive, TxId}) ->
    update_objects(Pid, Updates, {interactive, TxId}, proto_buf);
update_objects(Pid, Updates, {static, TxId}) ->
    update_objects(Pid, Updates, {static, TxId}, proto_buf).


update_objects(Pid, Updates, {interactive, TxId}, ReplyType) ->
    EncMsg = antidote_pb_codec:encode(update_objects, {Updates, TxId, ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec: decode_response(Result) of
                {opresponse, ok} -> ok;
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end;

update_objects(Pid, Updates, {static, TxId}, ReplyType) ->
    {Clock, Properties} = TxId,
    EncMsg = antidote_pb_codec:encode(static_update_objects,
                                      {Clock, Properties, Updates, ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {commit_transaction, CommitTimeStamp} ->
                    antidotec_pb_socket:store_commit_time(Pid, CommitTimeStamp),
                    ok;
                {error, Reason} -> {error, Reason}
            end
    end.
            

read_objects(Pid, Objects, {interactive, TxId}) ->
    read_objects(Pid, Objects, {interactive, TxId}, proto_buf);
read_objects(Pid, Objects, {static, TxId}) ->
    read_objects(Pid, Objects, {static, TxId}, proto_buf).

read_objects(Pid, Objects, {interactive, TxId}, ReplyType) ->
    EncMsg = antidote_pb_codec:encode(read_objects, {Objects, TxId, ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {read_objects, Values} ->
                    ResObjects = lists:map(fun({Type, Val}) ->
                                        Mod = antidotec_datatype:module_for_type(Type),
                                        Mod:new(Val)
                                end, Values),
                    {ok, ResObjects};
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end;
read_objects(Pid, Objects, {static, TxId}, ReplyType) ->
    {Clock, Properties} = TxId,
    EncMsg = antidote_pb_codec:encode(static_read_objects,
                                      {Clock, Properties, Objects, ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {static_read_objects_resp, Values, CommitTimeStamp} ->
                    antidotec_pb_socket:store_commit_time(Pid, CommitTimeStamp),
                    ResObjects = lists:map(
                                   fun({Type, Val}) ->
                                           Mod = antidotec_datatype:module_for_type(Type),
                                           Mod:new(Val)
                                   end, Values),
                    {ok, ResObjects};
                {error, Reason} -> {error, Reason}
            end
    end.



%% Legion stuff
get_objects(Pid, Objects, ReplyType) ->
    EncMsg = antidote_pb_codec:encode(get_objects, {Objects,ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {get_objects, Values} ->
                    ResObjects = lists:map(
                                   fun({object,Val}) ->
					   Val
                                   end, Values),
                    {ok, ResObjects};
                {error, Reason} -> {error, Reason}
            end
    end.

get_log_operations(Pid, ObjectClockTuple, ReplyType) ->
    EncMsg = antidote_pb_codec:encode(get_log_operations, {ObjectClockTuple,ReplyType}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {get_log_operations, Values} ->
		    lager:info("The get log ops result ~p", [Values]),
                    ResObjects = lists:map(
                                   fun(Operations) ->
					   lists:map(fun({opid_and_payload,Value}) ->
							     Value
						     end, Operations)
                                   end, Values),
                    {ok, ResObjects};
                {error, Reason} -> {error, Reason}
            end
    end.    
    
is_static(TxnProperties) ->
    case TxnProperties of
        [{static, true}] ->
            true;
        _ -> false
    end.
