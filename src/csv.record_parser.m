%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013 Julien Fischer.
% See the file COPYING for license details.
%-----------------------------------------------------------------------------%
% 
% Actually, should just be named csv.parser, but since the stdlib has
% a module by the name of parser that isn't a good idea.
%
%-----------------------------------------------------------------------------%

:- module csv.record_parser.
:- interface.

:- type client(Stream)
    --->    client_reader(csv.reader(Stream))
    ;       client_raw_reader(csv.raw_reader(Stream)).


:- type record_result(Error) == stream.result(list(string), csv.error(Error)).

:- pred get_next_record(client(Stream)::in, record_result(Error)::out,
    State::di, State::uo) is det 
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

%-----------------------------------------------------------------------------%
%
% Access information about the parser client.
%

:- func get_client_stream(client(Stream)) = Stream 
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_client_stream(client_reader(R)) = R ^ csv_stream.
get_client_stream(client_raw_reader(R)) = R ^ csv_raw_stream.

:- func get_client_field_limit(client(Stream)) = record_field_limit
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_client_field_limit(client_reader(R)) = R ^ csv_field_limit.
get_client_field_limit(client_raw_reader(R)) = R ^ csv_raw_field_limit.

:- func get_client_field_width(client(Stream)) = field_width_limit
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_client_field_width(client_reader(R)) = R ^ csv_width_limit.
get_client_field_width(client_raw_reader(R)) = R ^ csv_raw_width_limit.

:- func get_client_field_delimiter(client(Stream)) = char
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_client_field_delimiter(client_reader(R)) = R ^ csv_field_delimiter.
get_client_field_delimiter(client_raw_reader(R)) = R ^ csv_raw_delimiter.

%-----------------------------------------------------------------------------%
%
% Reading raw records.
%

    % Was the last character we saw when scanning the previous field
    % an unquoted delimiter.  
    % We need to keep track of this information in order to handle
    % unquoted empty trailing fields properly.
    %
:- type last_seen
    --->    last_seen_start
            % We have not started scanning yet.

    ;       last_seen_delimiter
            % The last character we saw was an unquoted delimiter.

    ;       last_seen_other.
            % The last character we saw was something else.

get_next_record(Reader, Result, !State) :-
    get_fields(Reader, [], Result, last_seen_start, _, 0, _NumFields, !State).

:- pred get_fields(client(Stream)::in, list(string)::in,
    record_result(Error)::out,
    last_seen::in, last_seen::out,
    int::in, int::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_fields(Reader, !.Fields, Result, !LastSeen, !FieldsRead, !State) :-
    NextFieldNum = !.FieldsRead + 1,
    next_raw_field(Reader, NextFieldNum, FieldResult, !LastSeen, !State),
    (
        FieldResult = fr_field(Field),
        !:FieldsRead = !.FieldsRead + 1,
        MaybeFieldLimit = get_client_field_limit(Reader),
        ( if
            MaybeFieldLimit = exactly(FieldLimit),
            !.FieldsRead > FieldLimit
        then
            Stream = get_client_stream(Reader),
            stream.name(Stream, Name, !State),
            stream.get_line(Stream, LineNo, !State),
            Error = csv_error(
                Name, 
                LineNo, 
                !.FieldsRead,
                "record field limit exceeded"
            ),
            Result = error(Error)
        else
            !:Fields = [Field | !.Fields],
            get_fields(Reader, !.Fields, Result, !LastSeen, !FieldsRead,
                !State) 
        )
    ;
        FieldResult = fr_error(Error),
        Result = error(Error)
    ;
        FieldResult = fr_end_of_record,
        list.reverse(!Fields),
        Result = ok(!.Fields)
    ;
        FieldResult = fr_eof,
        (
            !.Fields = [],
            Result = eof
        ;
            % The EOF was the terminating this record.
            !.Fields = [_ | _],
            list.reverse(!Fields),
            Result = ok(!.Fields)
        )
    ).

%-----------------------------------------------------------------------------%
%
% Reading raw fields.
%


:- type field_result(Error)
    --->    fr_field(string)
    ;       fr_error(csv.error(Error))
    ;       fr_end_of_record
    ;       fr_eof.

:- pred next_raw_field(client(Stream)::in, int::in,
    field_result(Error)::out, last_seen::in, last_seen::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

next_raw_field(Reader, FieldNum, FieldResult, !LastSeen, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, GetResult, !State),
    (
        GetResult = ok(Char),
        ( if
            Char = ('\n')
        then
            ( if !.LastSeen = last_seen_delimiter then
                stream.unget(Stream, '\n', !State),
                !:LastSeen = last_seen_other,
                FieldResult = fr_field("")
            else
                FieldResult = fr_end_of_record
            )
        else if
            % We allow empty fields here.
            Char = get_client_field_delimiter(Reader)
        then 
            !:LastSeen = last_seen_delimiter,
            FieldResult = fr_field("")
        else if
            Char = ('"')
        then
            !:LastSeen = last_seen_other,
            char_buffer.init(Buffer, !State),
            next_quoted_field(Reader, FieldNum, Buffer, FieldResult,
                !LastSeen, !State)
        else
            char_buffer.init(Buffer, !State),
            char_buffer.add(Buffer, Char, !State),
            next_unquoted_field(Reader, FieldNum, Buffer, FieldResult,
                !LastSeen, !State)
        )
    ;
        GetResult = eof,
        FieldResult = fr_eof
    ;
        GetResult = error(Error),
        FieldResult = fr_error(stream_error(Error))
    ).

:- pred next_quoted_field(client(Stream)::in, int::in,
    char_buffer::in, field_result(Error)::out,
    last_seen::in, last_seen::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

next_quoted_field(Reader, FieldNum, Buffer, Result, !LastSeen, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, GetResult, !State),
    (
        GetResult = ok(Char),
        ( if Char = ('"') then
            stream.get(Stream, NextGetResult, !State),
            (
                NextGetResult = ok(NextChar),
                % Double quotes are used to escape a quote.
                ( if 
                    NextChar = ('"')
                then
                    add(Buffer, Char, !State),
                    !:LastSeen = last_seen_other,
                    next_quoted_field(Reader, FieldNum, Buffer, Result,
                        !LastSeen, !State)
                else if
                    NextChar = get_client_field_delimiter(Reader)
                then
                    !:LastSeen = last_seen_delimiter,
                    Field = char_buffer.to_string(Buffer, !.State),
                    Result = fr_field(Field)
                else if
                    NextChar = ('\n')
                then
                    !:LastSeen = last_seen_other,
                    stream.unget(Stream, '\n', !State),
                    Field = char_buffer.to_string(Buffer, !.State),
                    Result = fr_field(Field)
                else if
                    NextChar = ('\r')
                then
                    stream.get(Stream, AfterCR_Result, !State),
                    (
                        AfterCR_Result = ok(AfterCR_Char),
                        ( if AfterCR_Char = ('\n') then
                            stream.unget(Stream, '\n', !State),
                            Field = char_buffer.to_string(Buffer, !.State),
                            Result = fr_field(Field)
                        else
                            stream.name(Stream, Name, !State),
                            stream.get_line(Stream, LineNo, !State),
                            Error = csv_error(
                                Name,
                                LineNo,
                                FieldNum,
                                "missing closing quote"
                            ),
                            Result = fr_error(Error)
                        )
                    ;
                        AfterCR_Result = eof,
                        stream.name(Stream, Name, !State),
                        stream.get_line(Stream, LineNo, !State),
                        Error = csv_error(Name, LineNo, FieldNum,
                            "unexpected end-of-file"),
                        Result = fr_error(Error)
                    ;
                        AfterCR_Result = error(Error),
                        Result = fr_error(stream_error(Error))
                    )
                else
                    stream.name(Stream, Name, !State),
                    stream.get_line(Stream, LineNo, !State),
                    Error = csv_error(
                        Name,
                        LineNo,
                        FieldNum,
                        "missing closing quote"
                    ),
                    Result = fr_error(Error)
                )
            ;
                % This might be the last field in the file, so don't expect a
                % newline.
                NextGetResult = eof,
                Field = char_buffer.to_string(Buffer, !.State),
                Result = fr_field(Field)
            ;
                NextGetResult = error(Error),
                Result = fr_error(stream_error(Error))
            )
        else 
            % NOTE; quoted delimiter characters do not count has having
            % seen a delimter.
            !:LastSeen = last_seen_other,
            add(Buffer, Char, !State),
            MaybeFieldWidthLimit = get_client_field_width(Reader),
            (
                MaybeFieldWidthLimit = no_limit,
                next_quoted_field(Reader, FieldNum, Buffer, Result, !LastSeen, !State)
            ;
                MaybeFieldWidthLimit = limited(Limit),
                NumChars = char_buffer.num_chars(Buffer, !.State),
                ( if NumChars > Limit then
                    stream.name(Stream, Name, !State),
                    stream.get_line(Stream, LineNo, !State),
                    Error = csv_error(Name, LineNo, FieldNum,
                        "field width limit exceeded"),
                    Result = fr_error(Error)
                else
                    next_quoted_field(Reader, FieldNum, Buffer, Result, !LastSeen, !State)
                )
            )
        )
    ;
        GetResult = eof,
        stream.name(Stream, Name, !State),
        stream.get_line(Stream, LineNo, !State),
        Error = csv_error(Name, LineNo, FieldNum, "missing closing quote"),
        Result = fr_error(Error)
    ; 
        GetResult = error(Error),
        Result = fr_error(stream_error(Error))
    ).

:- pred next_unquoted_field(client(Stream)::in, int::in,
    char_buffer::in, field_result(Error)::out, 
    last_seen::in, last_seen::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

next_unquoted_field(Reader, FieldNum, Buffer, Result, !LastSeen, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, GetResult, !State),
    (
        GetResult = ok(Char),
        ( if
            Char = get_client_field_delimiter(Reader)
        then
            !:LastSeen = last_seen_delimiter,
            Field = char_buffer.to_string(Buffer, !.State),
            Result = fr_field(Field)
        else if 
            Char = ('"')
        then
            stream.name(Stream, Name, !State),
            stream.get_line(Stream, LineNo, !State),
            Result = fr_error(csv_error(Name, LineNo, FieldNum,
                "unexpected quote"))
        else if
            Char = ('\n')
        then
            !:LastSeen = last_seen_other,
            stream.unget(Stream, '\n', !State),
            chomp_cr(Buffer, !State),
            Field = char_buffer.to_string(Buffer, !.State),
            Result = fr_field(Field)
        else
            !:LastSeen = last_seen_other,
            add(Buffer, Char, !State),
            MaybeFieldWidthLimit = get_client_field_width(Reader),
            (
                MaybeFieldWidthLimit = no_limit,
                next_unquoted_field(Reader, FieldNum, Buffer,
                    Result, !LastSeen, !State)
            ;
                MaybeFieldWidthLimit = limited(Limit),
                NumChars = char_buffer.num_chars(Buffer, !.State),
                ( if NumChars > Limit then
                    stream.name(Stream, Name, !State),
                    stream.get_line(Stream, LineNo, !State),
                    Error = csv_error(Name, LineNo, FieldNum,
                        "field width exceeded"),
                    Result = fr_error(Error)
                else
                    next_unquoted_field(Reader, FieldNum, Buffer,
                        Result, !LastSeen, !State)
                )
            )
        )
    ;
        % This might be the end of the file
        GetResult = eof,
        Field = char_buffer.to_string(Buffer, !.State),
        Result = fr_field(Field)
    ;
        GetResult = error(Error),
        Result = fr_error(stream_error(Error))
    ).

%-----------------------------------------------------------------------------%
:- end_module csv.record_parser.
%-----------------------------------------------------------------------------%
