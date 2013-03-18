%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013 Julien Fischer.
% See the file COPYING for license details.
%-----------------------------------------------------------------------------%

:- module csv.typed_reader.
:- interface.

%-----------------------------------------------------------------------------%

:- import_module bool.

%-----------------------------------------------------------------------------%

    % get_csv(Stream, Desc, Result, !State):
    % Reads a sequence of CSV records from Stream until EOF.
    % 
:- pred get_csv(csv.reader(Stream)::in, csv.result(csv, Error)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%-----------------------------------------------------------------------------%
%
% Folding over records.
%

:- pred record_fold(csv.reader(Stream), pred(record, T, T),
    T, csv.maybe_partial_res(T, Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode record_fold(in, in(pred(in, in, out) is det),
    in, out, di, uo) is det.
:- mode record_fold(in, in(pred(in, in, out) is cc_multi),
    in, out, di, uo) is cc_multi.

:- pred record_fold_state(csv.reader(Stream), pred(record, State, State),
    csv.res(Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode record_fold_state(in, in(pred(in, di, uo) is det),
    out, di, uo) is det.
:- mode record_fold_state(in, in(pred(in, di, uo) is cc_multi),
    out, di, uo) is cc_multi.

:- pred record_fold2_state(csv.reader(Stream), pred(record, T, T, State, State),
    T, csv.maybe_partial_res(T, Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode record_fold2_state(in, in(pred(in, in, out, di, uo) is det),
    in, out, di, uo) is det.
:- mode record_fold2_state(in, in(pred(in, in, out, di, uo) is cc_multi),
    in, out, di, uo) is cc_multi.

:- pred record_fold_maybe_stop(csv.reader(Stream), pred(record, bool, T, T),
    T, csv.maybe_partial_res(T, Error), State, State)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).
:- mode record_fold_maybe_stop(in, in(pred(in, out, in, out) is det),
    in, out, di, uo) is det.
:- mode record_fold_maybe_stop(in, in(pred(in, out, in, out) is cc_multi),
    in, out, di, uo) is cc_multi.

%-----------------------------------------------------------------------------%

    % get_header(Stream, Desc, Result, !State):
    % Reads a CSV header line from Stream.
    %
:- pred get_header(csv.reader(Stream)::in, csv.result(header, Error)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % get_record(Stream, Desc, Result, !State):
    % Read a single CSV record from Stream.
    % This assumes that any header has already been read.
    %
:- pred get_record(csv.reader(Stream)::in, csv.result(record, Error)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % get_records(Stream, Desc, Result, !State):
    % Read records from Stream until EOF.
    % The records are returned in the order in which they are read.
    % This assumes that any header has already been read.
    %
:- pred get_records(csv.reader(Stream)::in, csv.res(records, Error)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module float.
:- import_module parser.
:- import_module require.
:- import_module term_io.

%-----------------------------------------------------------------------------%

get_csv(Desc, Result, !State) :-
    Desc = csv_reader(Stream, HeaderDesc, _, _, _, _),
    stream.name(Stream, Name, !State),
    (
        HeaderDesc = no_header,
        get_records(Desc, RecordsResult, !State),
        (
            RecordsResult = ok(Records),
            Result = ok(csv(Name, no, Records))
        ;
            RecordsResult = error(Error),
            Result = error(Error)
        )
    ;
        HeaderDesc = header_desc(_),
        get_header(Desc, HeaderResult, !State),
        (
            HeaderResult = ok(Header),
            get_records(Desc, RecordsResult, !State),
            (
                RecordsResult = ok(Records),
                Result = ok(csv(Name, yes(Header), Records))
            ;
                RecordsResult = error(Error),
                Result = error(Error)
            )
        ;
            HeaderResult = eof,
            stream.get_line(Stream, LineNo, !State),
            FieldNum = 1,
            HeaderError = csv_error(Name, LineNo, FieldNum,
                "unexpected end-of-file in header"),
            Result = error(HeaderError)
        ;
            HeaderResult = error(HeaderError),
            Result = error(HeaderError)
        )
    ).

%-----------------------------------------------------------------------------%

get_records(Desc, Result, !State) :-
    record_fold(Desc, add_record, [], FoldResult, !State),
    (
        FoldResult = ok(RevRecords),
        list.reverse(RevRecords, Records),
        Result = ok(Records)
    ;
        FoldResult = error(_, Error),
        Result = error(Error)
    ).

:- pred add_record(record::in, records::in, records::out) is det.

add_record(Record, !Records) :-
    !:Records = [Record | !.Records].

%-----------------------------------------------------------------------------%

get_header(Reader, Result, !State) :-
    HeaderDesc = Reader ^ csv_header_desc,
    (
        HeaderDesc = no_header,
        unexpected($module, $pred, "CSV desc specifies no header")
    ;
        HeaderDesc = header_desc(_),
        stream.get_line(get_stream(Reader), LineNo, !State),
        get_next_record(client_reader(Reader), RecordResult, !State),
        (
            RecordResult = ok(RawHeader),
            RawHeader = raw_record(_, HeaderFields), 
            RecordDesc = Reader ^ csv_record_desc,
            list.length(RecordDesc, NumFields),
            list.length(HeaderFields, NumHeaderFields),
            compare(CmpResult, NumHeaderFields, NumFields),
            (
                CmpResult = (=),
                Header = header(HeaderFields),
                Result = ok(Header)
            ;
                ( CmpResult = (>)
                ; CmpResult = (<)
                ),
                stream.name(get_stream(Reader), StreamName, !State),
                string.format("expected %d fields in header: actual %d",
                    [i(NumFields), i(NumHeaderFields)], Msg),
                Error = csv_error(
                    StreamName,
                    LineNo,
                    NumHeaderFields,
                    Msg
                ),
                Result = error(Error)
            )
        ;
            RecordResult = eof,
            Result = eof
        ;
            RecordResult = error(Error),
            Result = error(Error)
        )
    ).

:- type process_record_result
    --->    prr_ok(list(field_value))
    ;       prr_error(int, string).

get_record(Reader, Result, !State) :-
    % Get the stream line number *before* we read in the next record.
    Stream = Reader ^ csv_stream,
    stream.get_line(Stream, LineNo, !State),
    stream.name(Stream, StreamName, !State),
    get_next_record(client_reader(Reader), RecordResult, !State),
    (
        RecordResult = ok(RawRecord),
        FieldDescs = Reader ^ csv_record_desc,
        FieldNum = 1,
        RawRecord = raw_record(_, RawFields),
        process_fields(StreamName, LineNo, FieldDescs, RawFields, FieldNum,
            [], ProcessFieldsResult),
        (
            ProcessFieldsResult = prr_ok(RevFields),
            list.reverse(RevFields, Fields),
            Record = record(LineNo, Fields),
            Result = ok(Record)
        ;
            ProcessFieldsResult = prr_error(ErrorFieldNum, ErrorMsg),
            Error = csv_error(StreamName, LineNo, ErrorFieldNum, ErrorMsg),
            Result = error(Error)
        )
    ;
        RecordResult = error(Error),
        Result = error(Error)
    ;
        RecordResult = eof,
        Result = eof
    ).

%-----------------------------------------------------------------------------%

:- type process_field_result
    --->    pfr_ok(field_value)
    ;       pfr_discard
    ;       pfr_error(int, string).

:- pred process_fields(stream.name::in, int::in,
    list(field_desc)::in, list(raw_field)::in,
    int::in, list(field_value)::in, process_record_result::out) is det.

process_fields(_, _, [], [], _, FieldValues, prr_ok(FieldValues)).
process_fields(_, _, [_ | _], [], _, _, _) :-
    unexpected($module, $pred, "argument length mismatch").
process_fields(_, _, [], [_ | _], _, _, _) :-
    unexpected($module, $pred, "argument length mimsatch").
process_fields(StreamName, LineNo, [Desc | Descs], [RawField | RawFields],
        FieldNum, FieldValues, MaybeResult) :-
    process_field(StreamName, LineNo, Desc, RawField, FieldNum,
        MaybeFieldResult),
    (
        MaybeFieldResult = pfr_ok(FieldValue),
        process_fields(StreamName, LineNo, Descs, RawFields, FieldNum + 1,
            [FieldValue | FieldValues], MaybeResult)
    ;
        MaybeFieldResult = pfr_discard,
        process_fields(StreamName, LineNo, Descs, RawFields, FieldNum + 1,
            FieldValues, MaybeResult)
    ;
        MaybeFieldResult = pfr_error(ErrorFieldNum, ErrorMsg),
        MaybeResult = prr_error(ErrorFieldNum, ErrorMsg)
    ).

:- pred process_field(stream.name::in, int::in, field_desc::in, raw_field::in,
    int::in, process_field_result::out) is det.

process_field(_, _, Desc, RawField, FieldNum, MaybeResult) :-
    Desc = discard(MaybeWidthLimit),
    (
        MaybeWidthLimit = no_limit,
        MaybeResult = pfr_discard
    ;
        MaybeWidthLimit = limited(Limit),
        string.length(RawField, RawFieldLength),
        ( if RawFieldLength > Limit
        then MaybeResult = pfr_error(FieldNum, "field width exceeded")
        else MaybeResult = pfr_discard
        )
    ).

process_field(StreamName, LineNo, Desc, RawField, FieldNum, MaybeResult) :-
    Desc = field_desc(Type, MaybeWidthLimit, TrimWS),
    (
        TrimWS = trim_whitespace,
        RawFieldPrime = string.strip(RawField)
    ;
        TrimWS = do_not_trim_whitespace,
        RawFieldPrime = RawField
    ),
    (
        MaybeWidthLimit = no_limit,
        process_field_apply_actions(StreamName, LineNo, Type,
            RawFieldPrime, FieldNum, MaybeResult)
    ;
        MaybeWidthLimit = limited(Limit),
        string.length(RawFieldPrime, RawFieldLength),
        ( if RawFieldLength > Limit then
            MaybeResult = pfr_error(FieldNum, "field width exceeded")
        else
            process_field_apply_actions(StreamName, LineNo, Type,
                RawFieldPrime, FieldNum, MaybeResult)
        )
    ).

:- pred process_field_apply_actions(stream.name::in, int::in, field_type::in,
    raw_field::in, int::in, process_field_result::out) is det.

process_field_apply_actions(StreamName, LineNo, Type, RawField, FieldNum,
        MaybeResult) :-
    ( 
        Type = bool(BoolHandler, Actions),
        process_field_apply_actions_bool(BoolHandler, Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = int(FloatIntHandler, Actions),
        process_field_apply_actions_int(FloatIntHandler, Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = float(Actions),
        process_field_apply_actions_float(Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = floatstr(Actions),
        process_field_apply_actions_floatstr(Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = string(Actions),
        process_field_apply_actions_string(Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = date(Format, Actions),
        process_field_apply_actions_date(Format, Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = date_time(Format, Actions),
        process_field_apply_actions_date_time(Format, Actions, RawField,
            FieldNum, MaybeResult)
    ;
        Type = term(Actions),
        process_field_apply_actions_term(StreamName, LineNo, Actions,
            RawField, FieldNum, MaybeResult)
    ;
        Type = univ(UnivHandler, Actions),
        process_field_apply_actions_univ(UnivHandler, Actions, RawField,
            FieldNum, MaybeResult)
    ).

%----------------------------------------------------------------------------%
%
% Process Boolean fields.
%

:- pred process_field_apply_actions_bool(bool_handler::in,
    field_actions(bool)::in, raw_field::in, int::in, 
    process_field_result::out) is det.

process_field_apply_actions_bool(BoolHandler, Actions, RawField,
        FieldNum, MaybeResult) :-
    MaybeField = BoolHandler(RawField),
    (
        MaybeField = ok(Bool),
        apply_field_actions(Actions, Bool, ActionResult),
        (
            ActionResult = ok(BoolPrime),
            MaybeResult = pfr_ok(bool(BoolPrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    ;
        MaybeField = error(BoolError),
        MaybeResult = pfr_error(FieldNum, BoolError)
    ).

%----------------------------------------------------------------------------%
%
% Process int fields.
%
        
:- pred process_field_apply_actions_int(float_int_handler::in,
    field_actions(int)::in, raw_field::in, int::in, 
    process_field_result::out) is det.

process_field_apply_actions_int(FloatIntHandler, Actions, RawField,
        FieldNum, MaybeResult) :-
    ( if string.to_int(RawField, Int) then
        apply_field_actions(Actions, Int, ActionResult),
        (
            ActionResult = ok(IntPrime),
            MaybeResult = pfr_ok(int(IntPrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    else 
        (
            FloatIntHandler = do_not_allow_floats, 
            MaybeResult = pfr_error(FieldNum, "not an integer field")
        ;
            FloatIntHandler = convert_float_to_int(FloatToIntFunc),
            ( if string.to_float(RawField, Float) then
                Int = FloatToIntFunc(Float),
                apply_field_actions(Actions, Int, ActionResult),
                (
                    ActionResult = ok(IntPrime),
                    MaybeResult = pfr_ok(int(IntPrime))
                ;
                    ActionResult = error(ActionError),
                    MaybeResult = pfr_error(FieldNum, ActionError)
                )
            else
                MaybeResult = pfr_error(FieldNum, "not an integer field")
                
            )
        )
    ).

%----------------------------------------------------------------------------%
% 
% Process float fields.
%

:- pred process_field_apply_actions_float(field_actions(float)::in,
    raw_field::in, int::in, process_field_result::out) is det.

process_field_apply_actions_float(Actions, RawField, FieldNum, MaybeResult) :-
    ( if string.to_float(RawField, Float) then
        apply_field_actions(Actions, Float, ActionResult),
        (
            ActionResult = ok(FloatPrime),
            MaybeResult = pfr_ok(float(FloatPrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    else
        MaybeResult = pfr_error(FieldNum, "not a float field")
    ).

%----------------------------------------------------------------------------%
%
% Process float-as-string fields.
%
        
:- pred process_field_apply_actions_floatstr(field_actions(string)::in,
    raw_field::in, int::in, process_field_result::out) is det.

process_field_apply_actions_floatstr(Actions, RawField, FieldNum,
        MaybeResult) :-
    % XXX we should just check that the float matches a valid
    % float or double literal rather than attempting to convert
    % it because in the spf grades we may not be able to convert it.
    ( if string.to_float(RawField, _) then
        apply_field_actions(Actions, RawField, ActionResult),
        (
            ActionResult = ok(RawFieldPrime),
            % Check that the resulting string is still a float
            % after any used-specified transformations.
            ( if string.to_float(RawFieldPrime, _)
            then MaybeResult = pfr_ok(floatstr(RawFieldPrime))
            else error("field is not a float after actions")
            )
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    else
        MaybeResult = pfr_error(FieldNum, "not a float field")
    ).

%----------------------------------------------------------------------------%
%
% Process string fields.
%

:- pred process_field_apply_actions_string(field_actions(string)::in,
    raw_field::in, int::in, process_field_result::out) is det.

process_field_apply_actions_string(Actions, RawField, FieldNum,
        MaybeResult) :-
    apply_field_actions(Actions, RawField, ActionResult),
    (
        ActionResult = ok(String),
        MaybeResult = pfr_ok(string(String))
    ;
        ActionResult = error(ActionError),
        MaybeResult = pfr_error(FieldNum, ActionError)
    ).

%----------------------------------------------------------------------------%
%
% Process date fields.
%

:- pred process_field_apply_actions_date(date_format::in,
    field_actions(date)::in, raw_field::in, int::in, process_field_result::out)
    is det.

process_field_apply_actions_date(Format, Actions, RawField, FieldNum,
        MaybeResult) :-
    (
        Format = yyyy_mm_dd(Separator),
        ConvertPred = yyyy_mm_dd_to_date
    ;
        Format = dd_mm_yyyy(Separator),
        ConvertPred = dd_mm_yyyy_to_date
    ;
        Format = mm_dd_yyyy(Separator),
        ConvertPred = mm_dd_yyyy_to_date
    ;
        Format = yyyy_b_dd(Separator),
        ConvertPred = yyyy_b_dd_to_date
    ;
        Format = dd_b_yyyy(Separator),
        ConvertPred = dd_b_yyyy_to_date
    ;
        Format = b_dd_yyyy(Separator),
        ConvertPred = b_dd_yyyy_to_date
    ),
    convert_date(ConvertPred, Separator, RawField, MaybeDate),
    (
        MaybeDate = ok(DateTime),
        apply_field_actions(Actions, DateTime, ActionResult),
        (
            ActionResult = ok(DateTimePrime),
            MaybeResult = pfr_ok(date(DateTimePrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    ;
        MaybeDate = error(DateError),
        MaybeResult = pfr_error(FieldNum, DateError)
    ).

:- pred convert_date(pred(list(string), date)::in(pred(in, out) is semidet),
    string::in, string::in, maybe_error(date)::out) is det.

convert_date(ConvertPred, Separator, DateStr, MaybeDate) :-
    DateComponentStrs = string.split_at_string(Separator, DateStr),
    ( if ConvertPred(DateComponentStrs, DateTime)
    then MaybeDate = ok(DateTime)
    else MaybeDate = error("not a valid date")
    ).

:- pred yyyy_mm_dd_to_date(list(string)::in, date::out) is semidet.

yyyy_mm_dd_to_date(ComponentStrs, DateTime) :-
    ComponentStrs = [YearStr, MonthStr, DayStr],
    string.to_int(YearStr, Year),
    string.to_int(MonthStr, MonthNum),
    string.to_int(DayStr, Day),
    int_to_month(MonthNum, Month),
    calendar.init_date(Year, Month, Day, 0, 0, 0, 0, DateTime).

:- pred dd_mm_yyyy_to_date(list(string)::in, date::out) is semidet.

dd_mm_yyyy_to_date(ComponentStrs, DateTime) :-
    ComponentStrs = [DayStr, MonthStr, YearStr],
    string.to_int(DayStr, Day),
    string.to_int(MonthStr, MonthNum),
    int_to_month(MonthNum, Month),
    string.to_int(YearStr, Year),
    calendar.init_date(Year, Month, Day, 0, 0, 0, 0, DateTime).

:- pred mm_dd_yyyy_to_date(list(string)::in, date::out) is semidet.

mm_dd_yyyy_to_date(ComponentStrs, DateTime) :-
    ComponentStrs = [MonthStr, DayStr, YearStr],
    string.to_int(MonthStr, MonthNum),
    int_to_month(MonthNum, Month),
    string.to_int(DayStr, Day),
    string.to_int(YearStr, Year),
    calendar.init_date(Year, Month, Day, 0, 0, 0, 0, DateTime).

:- pred yyyy_b_dd_to_date(list(string)::in, date::out) is semidet.

yyyy_b_dd_to_date(ComponentStrs, DateTime) :-
    ComponentStrs = [YearStr, MonthStr, DayStr],
    string.to_int(YearStr, Year),
    abbrev_name_to_month(MonthStr, Month),
    string.to_int(DayStr, Day),
    calendar.init_date(Year, Month, Day, 0, 0, 0, 0, DateTime).

:- pred dd_b_yyyy_to_date(list(string)::in, date::out) is semidet.

dd_b_yyyy_to_date(ComponentStrs, DateTime) :-
    ComponentStrs = [DayStr, MonthStr, YearStr],
    string.to_int(YearStr, Year),
    abbrev_name_to_month(MonthStr, Month),
    string.to_int(DayStr, Day),
    calendar.init_date(Year, Month, Day, 0, 0, 0, 0, DateTime).

:- pred b_dd_yyyy_to_date(list(string)::in, date::out) is semidet.

b_dd_yyyy_to_date(ComponentStrs, DateTime) :-
    ComponentStrs = [MonthStr, DayStr, YearStr],
    string.to_int(YearStr, Year),
    abbrev_name_to_month(MonthStr, Month),
    string.to_int(DayStr, Day),
    calendar.init_date(Year, Month, Day, 0, 0, 0, 0, DateTime).

:- pred int_to_month(int::in, month::out) is semidet.

int_to_month(1, january).
int_to_month(2, february).
int_to_month(3, march).
int_to_month(4, april).
int_to_month(5, may).
int_to_month(6, june).
int_to_month(7, july).
int_to_month(8, august).
int_to_month(9, september).
int_to_month(10, october).
int_to_month(11, november).
int_to_month(12, december).

    % XXX should have something like this in the standard library.
    %
:- pred abbrev_name_to_month(string::in, month::out) is semidet.

abbrev_name_to_month(AbbrevMonthName0, Month) :-
    AbbrevMonthName = string.capitalize_first(AbbrevMonthName0),
    abbrev_name_to_month_2(AbbrevMonthName, Month).

:- pred abbrev_name_to_month_2(string::in, month::out) is semidet.

abbrev_name_to_month_2("Jan",  january).
abbrev_name_to_month_2("Feb",  february).
abbrev_name_to_month_2("Mar",  march).
abbrev_name_to_month_2("Apr",  april).
abbrev_name_to_month_2("May",  may).
abbrev_name_to_month_2("June", june).
abbrev_name_to_month_2("Jun",  june).
abbrev_name_to_month_2("July", july).
abbrev_name_to_month_2("Jul",  july).
abbrev_name_to_month_2("Aug",  august).
abbrev_name_to_month_2("Sept", september).
abbrev_name_to_month_2("Sep",  september).
abbrev_name_to_month_2("Oct",  october).
abbrev_name_to_month_2("Nov",  november).
abbrev_name_to_month_2("Dec",  december).

%----------------------------------------------------------------------------%

:- pred process_field_apply_actions_date_time(date_time_format::in,
    field_actions(date)::in, raw_field::in, int::in, process_field_result::out)
    is det.

process_field_apply_actions_date_time(Format, Actions, RawField, FieldNum,
        MaybeResult) :-
    Format = mm_dd_yyyy_hh_mm(DateSep, DateTimeSep, TimeSep),
    % XXX there's no good reason for this restriction.
    ( if 
        ( DateSep = TimeSep
        ; DateSep = DateTimeSep
        ; DateTimeSep = TimeSep
        )
    then
        error("separators for date_times must be distinct")
    else
        true
    ),
    DateTimeComponentStrs = string.split_at_string(DateTimeSep, RawField),
    ( if
        mm_dd_yyyy_hh_mm_to_date(DateSep, TimeSep, DateTimeComponentStrs,
            DateTime)
    then
        apply_field_actions(Actions, DateTime, ActionResult),
        (
            ActionResult = ok(DateTimePrime),
            MaybeResult = pfr_ok(date_time(DateTimePrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    else
        MaybeResult = pfr_error(FieldNum, "not a valid date-time")
    ).
    
:- pred mm_dd_yyyy_hh_mm_to_date(string::in, string::in,
    list(string)::in, date::out) is semidet.

mm_dd_yyyy_hh_mm_to_date(DateSep, TimeSep, DateTimeComponentStrs,
        DateTime) :-
    DateTimeComponentStrs = [DateStr, TimeStr],
    DateComponentStrs = string.split_at_string(DateSep, DateStr),
    TimeComponentStrs = string.split_at_string(TimeSep, TimeStr),
    DateComponentStrs = [MonthStr, DayStr, YearStr],
    string.to_int(YearStr, Year),
    string.to_int(MonthStr, MonthNum),
    string.to_int(DayStr, Day),
    int_to_month(MonthNum, Month),
    TimeComponentStrs = [HourStr, MinuteStr],
    string.to_int(HourStr, Hour),
    string.to_int(MinuteStr, Minute),
    calendar.init_date(Year, Month, Day, Hour, Minute, 0, 0, DateTime).

%----------------------------------------------------------------------------%

:- pred process_field_apply_actions_term(stream.name::in, int::in,
    field_actions({varset, term})::in, raw_field::in, int::in,
    process_field_result::out) is det.

process_field_apply_actions_term(StreamName, LineNo, Actions, RawField,
        FieldNum, MaybeResult) :-
    parser.read_term_from_string(StreamName, RawField, _EndPos, ReadTerm),
    (
        ReadTerm = term(Varset, Term0),
        % XXX fix up the line number in the context so it matches that
        % of the line we just read from the CSV reader.
        % The standard library's parser module does not (yet) allow us
        % to set the line number when reading from a string.
        (
            Term0 = functor(F, Args, Context0),
            Context0 = term.context(File, _),
            Context = term.context(File, LineNo),
            Term = functor(F, Args, Context)
        ;
            Term0 = variable(Var, Context0),
            Context0 = term.context(File, _),
            Context = term.context(File, LineNo),
            Term = variable(Var, Context)
        ),
        apply_field_actions(Actions, {Varset, Term}, ActionResult),
        (
            ActionResult = ok({VarsetPrime, TermPrime}),
            MaybeResult = pfr_ok(term(VarsetPrime, TermPrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    ;
        % XXX I don't think this can occur when reading a term
        % from a string.
        ReadTerm = eof,
        MaybeResult = pfr_error(FieldNum, "not a term field")
    ;
        % Ignore the line number here since our caller will set the 
        % correct one (i.e. the one from the CSV reader stream).
        ReadTerm = error(TermErrorMsg, _),
        MaybeResult = pfr_error(FieldNum, TermErrorMsg)
    ).

%----------------------------------------------------------------------------%
%
% Process univ fields.
%

:- pred process_field_apply_actions_univ(univ_handler::in,
    field_actions(univ)::in, raw_field::in, int::in, 
    process_field_result::out) is det.

process_field_apply_actions_univ(UnivHandler, Actions, RawField,
        FieldNum, MaybeResult) :-
    MaybeField = UnivHandler(RawField),
    (
        MaybeField = ok(Univ),
        apply_field_actions(Actions, Univ, ActionResult),
        (
            ActionResult = ok(UnivPrime),
            MaybeResult = pfr_ok(univ(UnivPrime))
        ;
            ActionResult = error(ActionError),
            MaybeResult = pfr_error(FieldNum, ActionError)
        )
    ;
        MaybeField = error(UnivError),
        MaybeResult = pfr_error(FieldNum, UnivError)
    ).

%-----------------------------------------------------------------------------%

:- pred apply_field_actions(field_actions(T)::in,
    T::in, field_action_result(T)::out) is det.

apply_field_actions(Actions, Value, Result) :-
    (
        Actions = [],
        Result = ok(Value)
    ;
        Actions = [Action | ActionsPrime],
        ActionResult = Action(Value),
        (
            ActionResult = ok(ValuePrime),
            apply_field_actions(ActionsPrime, ValuePrime, Result)
        ;
            ActionResult = error(_),
            Result = ActionResult
        )
    ).

%-----------------------------------------------------------------------------%
%
% Folding over records.
%

record_fold(Desc, RecordAction, !.Acc, Result, !State) :-
    get_record(Desc, RecordResult, !State),
    (
        RecordResult = ok(Record),
        RecordAction(Record, !Acc),
        record_fold(Desc, RecordAction, !.Acc, Result, !State)
    ;
        RecordResult = eof,
        Result = ok(!.Acc)
    ;
        RecordResult = error(Error),
        Result = error(!.Acc, Error)
    ).

record_fold_state(Desc, RecordAction, Result, !State) :-
    get_record(Desc, RecordResult, !State),
    (
        RecordResult = ok(Record),
        RecordAction(Record, !State),
        record_fold_state(Desc, RecordAction, Result, !State)
    ;
        RecordResult = eof,
        Result = ok
    ;
        RecordResult = error(Error),
        Result = error(Error)
    ).

record_fold2_state(Desc, RecordAction, !.Acc, Result, !State) :-
    get_record(Desc, RecordResult, !State),
    (
        RecordResult = ok(Record),
        RecordAction(Record, !Acc, !State),
        record_fold2_state(Desc, RecordAction, !.Acc, Result, !State)
    ;
        RecordResult = eof,
        Result = ok(!.Acc)
    ;
        RecordResult = error(Error),
        Result = error(!.Acc, Error)
    ).

record_fold_maybe_stop(Desc, Pred, !.Acc, Result, !State) :-
    get_record(Desc, RecordResult, !State),
    (
        RecordResult = ok(Record),
        Pred(Record, Continue, !Acc),
        (
            Continue = yes,
            record_fold_maybe_stop(Desc, Pred, !.Acc, Result, !State)
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

%-----------------------------------------------------------------------------%
:- end_module csv.typed_reader.
%-----------------------------------------------------------------------------%
