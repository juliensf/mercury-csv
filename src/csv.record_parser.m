%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013-2018 Julien Fischer.
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

:- type record_result(Error) == stream.result(raw_record, csv.error(Error)).

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

:- func get_client_comments(client(Stream)) = comments
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_client_comments(client_reader(R)) = R ^ csv_comments.
get_client_comments(client_raw_reader(_)) = no_comments.

:- func get_client_quotation_mark_in_unquoted_field(client(Stream)) =
    quotation_mark_in_unquoted_field
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_client_quotation_mark_in_unquoted_field(client_reader(R)) =
    R ^ csv_quotation_mark_in_unquoted_field.
get_client_quotation_mark_in_unquoted_field(client_raw_reader(R)) =
    R ^ csv_raw_quotation_mark_in_unquoted_field.

%-----------------------------------------------------------------------------%
%
% Reading raw records.
%

    % Was the last character we saw when scanning the previous field an
    % unquoted delimiter.
    % We need to keep track of this information in order to handle unquoted
    % empty trailing fields properly.
    %
:- type last_seen
    --->    last_seen_start
            % We have not started scanning yet.

    ;       last_seen_delimiter
            % The last character we saw was an unquoted delimiter.

    ;       last_seen_other.
            % The last character we saw was something else.

get_next_record(Reader, Result, !State) :-
    get_fields(Reader, [], FieldsResult, last_seen_start, _, 0, _NumFields,
        0, _ColNo, !State),
    (
        FieldsResult = fsr_fields(RawRecord),
        Result = ok(RawRecord)
    ;
        FieldsResult = fsr_comment_line,
        get_next_record(Reader, Result, !State)
    ;
        FieldsResult = fsr_eof,
        Result = eof
    ;
        FieldsResult = fsr_error(Error),
        Result = error(Error)
    ).

:- type fields_result(Error)
    --->    fsr_fields(raw_record)
    ;       fsr_comment_line
    ;       fsr_eof
    ;       fsr_error(csv.error(Error)).

:- pred get_fields(client(Stream)::in, raw_fields::in,
    fields_result(Error)::out, last_seen::in, last_seen::out,
    int::in, int::out, column_number::in, column_number::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

get_fields(Reader, !.Fields, Result, !LastSeen, !FieldsRead, !ColNo, !State) :-
    NextFieldNo = !.FieldsRead + 1,
    stream.get_line(Stream, StartLineNo, !State),
    next_raw_field(Reader, StartLineNo, NextFieldNo, FieldResult, !LastSeen,
        !ColNo, !State),
    Stream = get_client_stream(Reader),
    % NOTE: LineNo is not necessarily the same as StartLineNo since quoted
    % fields can contain newlines.
    stream.get_line(Stream, LineNo, !State),
    (
        FieldResult = fr_field(Field),
        !:FieldsRead = !.FieldsRead + 1,
        MaybeFieldLimit = get_client_field_limit(Reader),
        ( if
            MaybeFieldLimit = exactly(FieldLimit),
            !.FieldsRead > FieldLimit
        then
            stream.name(Stream, Name, !State),
            StartCol = Field ^ raw_field_col_no,
            Error = csv_error(
                Name,
                LineNo,
                StartCol,
                !.FieldsRead,
                "record field limit exceeded"
            ),
            Result = fsr_error(Error)
        else
            !:Fields = [Field | !.Fields],
            get_fields(Reader, !.Fields, Result, !LastSeen, !FieldsRead,
                !ColNo, !State)
        )
    ;
        FieldResult = fr_comment_line,
        Result = fsr_comment_line
    ;
        FieldResult = fr_error(Error),
        Result = fsr_error(Error)
    ;
        FieldResult = fr_end_of_record,
        list.reverse(!Fields),
        Result = fsr_fields(raw_record(StartLineNo, !.Fields))
    ;
        FieldResult = fr_eof,
        (
            !.Fields = [],
            Result = fsr_eof
        ;
            % The EOF was the terminating this record.
            !.Fields = [_ | _],
            list.reverse(!Fields),
            Result = fsr_fields(raw_record(StartLineNo, !.Fields))
        )
    ).

%-----------------------------------------------------------------------------%
%
% Reading raw fields.
%

:- type field_result(Error)
    --->    fr_field(raw_field)
    ;       fr_error(csv.error(Error))
    ;       fr_comment_line
    ;       fr_end_of_record
    ;       fr_eof.

:- pred next_raw_field(client(Stream)::in, line_number::in, field_number::in,
    field_result(Error)::out, last_seen::in, last_seen::out,
    column_number::in, column_number::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

next_raw_field(Reader, StartLineNo, FieldNo, FieldResult, !LastSeen,
        !ColNo, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, GetResult, !State),
    (
        GetResult = ok(Char),
        increment_col_no(!ColNo),
        ( if
            Char = ('\n')
        then
            ( if !.LastSeen = last_seen_delimiter then
                stream.unget(Stream, '\n', !State),
                !:LastSeen = last_seen_other,
                Field = raw_field("", StartLineNo, !.ColNo),
                FieldResult = fr_field(Field)
            else
                FieldResult = fr_end_of_record
            )
        else if
            % We allow empty fields here.
            Char = get_client_field_delimiter(Reader)
        then
            !:LastSeen = last_seen_delimiter,
            Field = raw_field("", StartLineNo, !.ColNo),
            FieldResult = fr_field(Field)
        else if
            Comments = get_client_comments(Reader),
            Comments = allow_comments(CommentChar),
            Char = CommentChar
        then
            consume_until_next_nl_or_eof(Reader, CommentResult, !State),
            (
                CommentResult = ok,
                ( if !.LastSeen = last_seen_start then
                    FieldResult = fr_comment_line
                else
                    FieldResult = fr_end_of_record
                )
            ;
                CommentResult = error(Error),
                FieldResult = fr_error(Error)
            )
        else if
            Char = ('"')
        then
            !:LastSeen = last_seen_other,
            char_buffer.init(Buffer, !State),
            next_quoted_field(Reader, StartLineNo, !.ColNo, FieldNo,
                Buffer, FieldResult, !LastSeen, !ColNo, !State)
        else
            char_buffer.init(Buffer, !State),
            char_buffer.add(Buffer, Char, !State),
            next_unquoted_field(Reader, StartLineNo, !.ColNo, FieldNo,
                Buffer, FieldResult, !LastSeen, !ColNo, !State)
        )
    ;
        GetResult = eof,
        FieldResult = fr_eof
    ;
        GetResult = error(Error),
        FieldResult = fr_error(stream_error(Error))
    ).

:- pred next_quoted_field(client(Stream)::in, line_number::in,
    column_number::in, field_number::in, char_buffer::in,
    field_result(Error)::out, last_seen::in, last_seen::out,
    column_number::in, column_number::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

next_quoted_field(Reader, StartLineNo, StartColNo, FieldNo, Buffer,
        Result, !LastSeen, !ColNo, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, GetResult, !State),
    (
        GetResult = ok(Char),
        increment_col_no(!ColNo),
        ( if Char = ('"') then
            stream.get(Stream, NextGetResult, !State),
            (
                NextGetResult = ok(NextChar),
                increment_col_no(!ColNo),
                % Double quotes are used to escape a quote.
                ( if
                    NextChar = ('"')
                then
                    add(Buffer, Char, !State),
                    !:LastSeen = last_seen_other,
                    next_quoted_field(Reader, StartLineNo, StartColNo, FieldNo,
                        Buffer, Result, !LastSeen, !ColNo, !State)
                else if
                    NextChar = get_client_field_delimiter(Reader)
                then
                    !:LastSeen = last_seen_delimiter,
                    FieldValue = char_buffer.to_string(Buffer, !.State),
                    Field = raw_field(FieldValue, StartLineNo, StartColNo),
                    Result = fr_field(Field)
                else if
                    NextChar = ('\n')
                then
                    !:LastSeen = last_seen_other,
                    stream.unget(Stream, '\n', !State),
                    FieldValue = char_buffer.to_string(Buffer, !.State),
                    Field = raw_field(FieldValue, StartLineNo, StartColNo),
                    Result = fr_field(Field)
                else if
                    NextChar = ('\r')
                then
                    stream.get(Stream, AfterCR_Result, !State),
                    (
                        AfterCR_Result = ok(AfterCR_Char),
                        ( if AfterCR_Char = ('\n') then
                            stream.unget(Stream, '\n', !State),
                            FieldValue = char_buffer.to_string(Buffer,
                                !.State),
                            Field = raw_field(FieldValue, StartLineNo,
                                StartColNo),
                            Result = fr_field(Field)
                        else
                            increment_col_no(!ColNo),
                            stream.name(Stream, Name, !State),
                            Error = csv_error(
                                Name,
                                StartLineNo,
                                StartColNo,
                                FieldNo,
                                "missing closing quote"
                            ),
                            Result = fr_error(Error)
                        )
                    ;
                        AfterCR_Result = eof,
                        stream.name(Stream, Name, !State),
                        stream.get_line(Stream, LineNo, !State),
                        Error = csv_error(Name, LineNo, !.ColNo, FieldNo,
                            "unexpected end-of-file"),
                        Result = fr_error(Error)
                    ;
                        AfterCR_Result = error(Error),
                        Result = fr_error(stream_error(Error))
                    )
                else
                    stream.name(Stream, Name, !State),
                    Error = csv_error(
                        Name,
                        StartLineNo,
                        StartColNo,
                        FieldNo,
                        "missing closing quote"
                    ),
                    Result = fr_error(Error)
                )
            ;
                % This might be the last field in the file, so don't expect a
                % newline.
                NextGetResult = eof,
                FieldValue = char_buffer.to_string(Buffer, !.State),
                Field = raw_field(FieldValue, StartLineNo, StartColNo),
                Result = fr_field(Field)
            ;
                NextGetResult = error(Error),
                Result = fr_error(stream_error(Error))
            )
        else
            % NOTE: quoted delimiter characters do not count has having
            % seen a delimiter.
            !:LastSeen = last_seen_other,
            add(Buffer, Char, !State),
            % Reset the column number if we see a newline.
            ( if Char = ('\n') then !:ColNo = 0 else true ),
            MaybeFieldWidthLimit = get_client_field_width(Reader),
            (
                MaybeFieldWidthLimit = no_limit,
                next_quoted_field(Reader, StartLineNo, StartColNo, FieldNo,
                    Buffer, Result, !LastSeen, !ColNo, !State)
            ;
                MaybeFieldWidthLimit = limited(Limit),
                NumChars = char_buffer.num_chars(Buffer, !.State),
                ( if NumChars > Limit then
                    stream.name(Stream, Name, !State),
                    Error = csv_error(Name, StartLineNo, StartColNo, FieldNo,
                        "field width limit exceeded"),
                    Result = fr_error(Error)
                else
                    next_quoted_field(Reader, StartLineNo, StartColNo, FieldNo,
                        Buffer, Result, !LastSeen, !ColNo, !State)
                )
            )
        )
    ;
        GetResult = eof,
        stream.name(Stream, Name, !State),
        Error = csv_error(Name, StartLineNo, StartColNo, FieldNo,
             "missing closing quote"),
        Result = fr_error(Error)
    ;
        GetResult = error(Error),
        Result = fr_error(stream_error(Error))
    ).

:- pred next_unquoted_field(client(Stream)::in, line_number::in,
    column_number::in, field_number::in, char_buffer::in,
    field_result(Error)::out, last_seen::in, last_seen::out,
    column_number::in, column_number::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

next_unquoted_field(Reader, StartLineNo, StartColNo, FieldNo, Buffer,
        Result, !LastSeen, !ColNo, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, GetResult, !State),
    (
        GetResult = ok(Char),
        increment_col_no(!ColNo),
        ( if
            Char = get_client_field_delimiter(Reader)
        then
            !:LastSeen = last_seen_delimiter,
            FieldValue = char_buffer.to_string(Buffer, !.State),
            Field = raw_field(FieldValue, StartLineNo, StartColNo),
            Result = fr_field(Field)
        else if
            Char = ('"'),
            QuotationMarkInUnquotedField =
                get_client_quotation_mark_in_unquoted_field(Reader),
            QuotationMarkInUnquotedField = no_quotation_mark_in_unquoted_field
        then
             stream.name(Stream, Name, !State),
             stream.get_line(Stream, LineNo, !State),
             Result = fr_error(csv_error(Name, LineNo, !.ColNo, FieldNo,
                 "unexpected quote"))
        else if
            Char = ('\n')
        then
            !:LastSeen = last_seen_other,
            stream.unget(Stream, '\n', !State),
            chomp_cr(Buffer, !State),
            FieldValue = char_buffer.to_string(Buffer, !.State),
            Field = raw_field(FieldValue, StartLineNo, StartColNo),
            Result = fr_field(Field)
        else
            !:LastSeen = last_seen_other,
            add(Buffer, Char, !State),
            MaybeFieldWidthLimit = get_client_field_width(Reader),
            (
                MaybeFieldWidthLimit = no_limit,
                next_unquoted_field(Reader, StartLineNo, StartColNo, FieldNo,
                    Buffer, Result, !LastSeen, !ColNo, !State)
            ;
                MaybeFieldWidthLimit = limited(Limit),
                NumChars = char_buffer.num_chars(Buffer, !.State),
                ( if NumChars > Limit then
                    stream.name(Stream, Name, !State),
                    stream.get_line(Stream, LineNo, !State),
                    Error = csv_error(Name, LineNo, StartColNo, FieldNo,
                        "field width exceeded"),
                    Result = fr_error(Error)
                else
                    next_unquoted_field(Reader, StartLineNo, StartColNo,
                        FieldNo, Buffer, Result, !LastSeen, !ColNo, !State)
                )
            )
        )
    ;
        % This might be the end of the file
        GetResult = eof,
        FieldValue = char_buffer.to_string(Buffer, !.State),
        Field = raw_field(FieldValue, StartLineNo, StartColNo),
        Result = fr_field(Field)
    ;
        GetResult = error(Error),
        Result = fr_error(stream_error(Error))
    ).


%-----------------------------------------------------------------------------%

:- pred consume_until_next_nl_or_eof(client(Stream)::in,
    csv.res(Error)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

consume_until_next_nl_or_eof(Reader, Result, !State) :-
    Stream = get_client_stream(Reader),
    stream.get(Stream, ReadResult, !State),
    (
        ReadResult = ok(Char),
        ( if Char = ('\n') then
            Result = ok
        else
            consume_until_next_nl_or_eof(Reader, Result, !State)
        )
    ;
        ReadResult = eof,
        Result = ok
    ;
        ReadResult = error(Error),
        Result = error(stream_error(Error))
    ).

%-----------------------------------------------------------------------------%

:- pred increment_col_no(column_number::in, column_number::out) is det.

increment_col_no(I, I + 1).

%-----------------------------------------------------------------------------%
:- end_module csv.record_parser.
%-----------------------------------------------------------------------------%
