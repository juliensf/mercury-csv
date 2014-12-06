%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013-2014 Julien Fischer.
% See the file COPYING for license details.
%-----------------------------------------------------------------------------%

:- module csv.raw_reader.
:- interface.

:- import_module bool.

%-----------------------------------------------------------------------------%

:- pred get_raw_record(csv.raw_reader(Stream)::in,
    csv.result(raw_record, Error)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

:- pred get_raw_csv(csv.raw_reader(Stream)::in,
    csv.result(raw_csv, Error)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%-----------------------------------------------------------------------------%

:- pred fold(csv.raw_reader(Stream), pred(raw_record, T, T),
    T, csv.maybe_partial_res(T, Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode fold(in, in(pred(in, in, out) is det),
    in, out, di, uo) is det.
:- mode fold(in, in(pred(in, in, out) is cc_multi),
    in, out, di, uo) is cc_multi.

:- pred fold_state(csv.raw_reader(Stream),
    pred(raw_record, State, State), csv.res(Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode fold_state(in, in(pred(in, di, uo) is det),
    out, di, uo) is det.
:- mode fold_state(in, in(pred(in, di, uo) is cc_multi),
    out, di, uo) is cc_multi.

:- pred fold2_state(csv.raw_reader(Stream),
    pred(raw_record, T, T, State, State), T, csv.maybe_partial_res(T, Error),
    State, State) <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode fold2_state(in, in(pred(in, in, out, di, uo) is det),
    in, out, di, uo) is det.
:- mode fold2_state(in, in(pred(in, in, out, di, uo) is cc_multi),
    in, out, di, uo) is cc_multi.

:- pred fold_maybe_stop(csv.raw_reader(Stream), pred(raw_record, bool, T, T),
    T, csv.maybe_partial_res(T, Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode fold_maybe_stop(in, in(pred(in, out, in, out) is det),
    in, out, di, uo) is det.
:- mode fold_maybe_stop(in, in(pred(in, out, in, out) is cc_multi),
    in, out, di, uo) is cc_multi.

:- pred fold_state_maybe_stop(csv.raw_reader(Stream),
    pred(raw_record, bool, State, State), csv.res(Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode fold_state_maybe_stop(in, in(pred(in, out, di, uo) is det),
    out, di, uo) is det.
:- mode fold_state_maybe_stop(in, in(pred(in, out, di, uo) is cc_multi),
    out, di, uo) is cc_multi.

:- pred fold2_state_maybe_stop(csv.raw_reader(Stream),
    pred(raw_record, bool, T, T, State, State), T,
    csv.maybe_partial_res(T, Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode fold2_state_maybe_stop(in,
    in(pred(in, out, in, out, di, uo) is det), in, out,
    di, uo) is det.
:- mode fold2_state_maybe_stop(in,
    in(pred(in, out, in, out, di, uo) is cc_multi), in, out,
    di, uo) is cc_multi.

%-----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%

:- implementation.

%----------------------------------------------------------------------------%

get_raw_record(Reader, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Result = ok(RawRecord)
    ;
        RecordResult = eof,
        Result = eof
    ;
        RecordResult = error(Error),
        Result = error(Error)
    ).

%----------------------------------------------------------------------------%

get_raw_csv(Reader, Result, !State) :-
    raw_reader.fold(Reader, list.cons, [], FoldResult, !State),
    (
        FoldResult = ok(RevRawRecords),
        list.reverse(RevRawRecords, RawRecords),
        Result = ok(raw_csv(RawRecords))
    ;
        FoldResult = error(_, Error),
        Result = error(Error)
    ).

%----------------------------------------------------------------------------%

fold(Reader, Pred, !.Acc, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Pred(RawRecord, !Acc),
        fold(Reader, Pred, !.Acc, Result, !State)
    ;
        RecordResult = eof,
        Result = ok(!.Acc)
    ;
        RecordResult = error(Error),
        Result = error(!.Acc, Error) 
    ).

fold_state(Reader, Pred, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Pred(RawRecord, !State),
        fold_state(Reader, Pred, Result, !State)
    ;
        RecordResult = eof,
        Result = ok
    ;
        RecordResult = error(Error),
        Result = error(Error)
    ).

fold2_state(Reader, Pred, !.Acc, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Pred(RawRecord, !Acc, !State),
        fold2_state(Reader, Pred, !.Acc, Result, !State)
    ;
        RecordResult = eof,
        Result = ok(!.Acc)
    ;
        RecordResult = error(Error),
        Result = error(!.Acc, Error)
    ).

fold_maybe_stop(Reader, Pred, !.Acc, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Pred(RawRecord, Continue, !Acc),
        (
            Continue = yes,
            fold_maybe_stop(Reader, Pred, !.Acc, Result, !State)
        ;
            Continue = no,
            Result = ok(!.Acc)
        )
    ;
        RecordResult = eof,
        Result = ok(!.Acc)
    ;
        RecordResult = error(Error),
        Result = error(!.Acc, Error) 
    ).

fold_state_maybe_stop(Reader, Pred, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Pred(RawRecord, Continue, !State),
        (
            Continue = yes,
            fold_state_maybe_stop(Reader, Pred, Result, !State)
        ;
            Continue = no,
            Result = ok
        )
    ;
        RecordResult = eof,
        Result = ok
    ;
        RecordResult = error(Error),
        Result = error(Error) 
    ).

fold2_state_maybe_stop(Reader, Pred, !.Acc, Result, !State) :-
    Client = client_raw_reader(Reader),
    get_next_record(Client, RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        Pred(RawRecord, Continue, !Acc, !State),
        (
            Continue = yes,
            fold2_state_maybe_stop(Reader, Pred, !.Acc, Result, !State)
        ;
            Continue = no,
            Result = ok(!.Acc)
        )
    ;
        RecordResult = eof,
        Result = ok(!.Acc)
    ;
        RecordResult = error(Error),
        Result = error(!.Acc, Error) 
    ).

%----------------------------------------------------------------------------%
:- end_module csv.raw_reader.
%----------------------------------------------------------------------------%
