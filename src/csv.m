%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013-2017 Julien Fischer.
% See the file COPYING for license details.
%-----------------------------------------------------------------------------%
%
% Author: Julien Fischer <juliensf@gmail.com>
%
% A Mercury library for reading comma-separated value (CSV) data from streams.
%
%-----------------------------------------------------------------------------%

:- module csv.
:- interface.

%-----------------------------------------------------------------------------%

:- include_module csv.raw_reader.
:- include_module csv.typed_reader.

:- import_module bool.
:- import_module calendar.
:- import_module char.
:- import_module io.
:- import_module list.
:- import_module maybe.
:- import_module stream.
:- import_module univ.

%----------------------------------------------------------------------------%
%
% CSV errors.
%

:- type line_number == int.
:- type column_number == int.
:- type field_number == int.

    % This type describes errors that can occur while processing a CSV
    % stream.
    %
:- type csv.error(Error)
    --->    stream_error(Error)
            % An error has occurred in the underlying character stream.

    ;       csv_error(
                csv_err_name  :: stream.name,
                csv_err_line  :: line_number,
                csv_err_col   :: column_number,
                csv_err_field :: field_number,
                csv_err_msg   :: string
            ).
            % There is an error in the structure of the CSV file or
            % an error has occurred while processing the field values.

%----------------------------------------------------------------------------%
%
% Field delimiter selection.
%

    % Succeeds iff the given character cannot be used as a field delimiter.
    %
:- pred is_invalid_delimiter(char::in) is semidet.

    % Exceptions of this type are thrown if an invalid field delimiter
    % is passed to one of the reader initialization functions below.
    %
:- type invalid_field_delimiter_error
    --->    invalid_field_delimiter_error(char).

%----------------------------------------------------------------------------%
%
% Wrappers for the standard stream result types.
%

:- type csv.res(T, Error) == stream.res(T, csv.error(Error)).

:- type csv.res(Error) == stream.res(csv.error(Error)).

:- type csv.result(T, Error) == stream.result(T, csv.error(Error)).

:- type csv.maybe_partial_res(T, Error) ==
    stream.maybe_partial_res(T, csv.error(Error)).

%----------------------------------------------------------------------------%

    % What limit, if any, is imposed on the number of fields that may
    % appear in a record?
    %
:- type record_field_limit
    --->    no_limit
    ;       exactly(int).

    % What limit, if any, is imposed on the number of characters that may
    % appear in a field.  This limit is exclusive of the quote characters
    % for quoted fields?
    %
:- type field_width_limit
    --->    no_limit
    ;       limited(int).

    % Should leading- and trailing-whitespace be trimmed from a field?
    %
:- type trim_whitespace
    --->    trim_whitespace
    ;       do_not_trim_whitespace.

    % Should lines consisting of nothing but whitespace be ignored?
    %
:- type ignore_blank_lines
    --->    ignore_blank_lines
    ;       do_not_ignore_blank_lines.

%----------------------------------------------------------------------------%
%
% CSV reader.
%

    % A CSV reader gets CSV records from an underlying character stream,
    % optionally applies some post-processing to the field values and then
    % returns the field values as lists of Mercury data types.
    %
:- type reader(Stream).

:- type reader_params
    --->    reader_params(
                ignore_blank_lines :: ignore_blank_lines,
                field_delimiter    :: char
            ).

    % This type specifies whether the CSV data begins with a header line or
    % not.
    %
:- type header_desc
    --->    no_header
    ;       header_desc(
                header_width_limit :: field_width_limit
            ).

:- type record_desc == list(field_desc).

:- type field_desc
    --->    discard(field_width_limit)
            % The field should be discarded and not returned as part of the
            % records.  Note that if there is a field width limit it will still
            % be enforced even though the field is being discarded.
            %
            % XXX fields in the header for discarded fields currently remain in
            % the header.

    ;       field_desc(
                field_type :: field_type,
                % What type of Mercury value does this field represent?  Also,
                % any details of any optional checking or transformation that
                % should be performed on the field value.

                field_width_limit :: field_width_limit,
                % What, if any, is the limit on the number of characters that
                % this field may contain?  For quoted fields, the character
                % count does not include the quote characters.

                field_trim_whitespace :: trim_whitespace
                % Should we trim leading- and trailing-whitespace from this
                % field?  Such trimming is done before any other processing.
            ).

    % Most field_types may have some actions associated with them.
    % These actions can be used to check or transform the field value.
    % Field actions are applied in the order given and only as far as the first
    % action that returns an error.
    %
:- type field_type
    --->    bool(bool_handler, field_actions(bool))
            % This field is a Mercury Boolean (i.e. of type bool.bool/0).
            % The user-provided "bool_handler" function is responsible for
            % converting the field's string representation into a bool.bool/0
            % value.

    ;       int(float_int_handler, field_actions(int))
            % This field is a Mercury int value.
            % The "float_int_handler" argument specifies what should be done if
            % a float value is encountered in the field.

    ;       float(field_actions(float))
            % The field is a Mercury float value.

    ;       floatstr(field_actions(string))
            % The field represents a float value, but we return it as Mercury
            % string.
            % NOTE: after applying any user actions the value is checked again.
            % An error is returned if the string no longer represents a float.

    ;       string(field_actions(string))
            % A Mercury string.

    ;       date(date_format, field_actions(date))
            % A Mercury calander.date/0 value.
            % The time component of the resulting date is always set to
            % midnight.  (XXX because the standard library doesn't have type
            % that represents only dates without a time component.)

    ;       date_time(date_time_format, field_actions(date))
            % A Mercury calander.date/0 value.
            % The time component is set as specified.
            % XXX support for this is currently very limited.

    ;       univ(univ_handler, field_actions(univ))
            % A Mercury univ/0 value.
            % The user provided "univ_handler" function is responsible for
            % converting the field's string representation into a univ.

    ;       maybe(field_type).
            % A Mercury maybe/0 value.
            % A blank field in the CSV data corresponds to maybe.no/0.
            % A software_error/0 exception is thrown if nested maybe fields
            % are encountered.

:- type field_action(T) == (func(T) = field_action_result(T)).
:- type field_actions(T) == list(field_action(T)).

:- type field_action_result(T) == maybe_error(T).

    % Handler functions convert a field's string representation into
    % a value of the required Mercury type.
    %
:- type handler(T) == (func(string) = maybe_error(T)).

:- type bool_handler == handler(bool).
:- type univ_handler == handler(univ).

    % This type specifies what we should do if we encounter a CSV
    % field that contains a float when the descriptor says that
    % it should contain an int.
    %
:- type float_int_handler
    --->    do_not_allow_floats
            % Do not allow a float where an int is expected.
            % The reader will return an error in this case.

    ;       convert_float_to_int(func(float) = int).
            % Use the provided function to convert the float into an int.

    % This type specifies the date formats that are accepted by the reader.
    % The string argument specifies how the date components are separated.
    %
:- type date_format
    --->    yyyy_mm_dd(string)  % e.g. 2013-03-06.
    ;       dd_mm_yyyy(string)  % e.g. 06-03-2013.
    ;       mm_dd_yyyy(string)  % e.g. 03-06-2013.
    ;       yyyy_b_dd(string)   % e.g. 2013-Mar-03.
    ;       dd_b_yyyy(string)   % e.g. 03-Mar-2013.
    ;       b_dd_yyyy(string).  % e.g. Mar-03-2013.


    % The first string argument specifies how the date components are
    % separated.
    % The second string argument specifies how the date and time components
    % are separated.
    % The third argument specifies how the time components are separated.
    % XXX currently all three must be different from each other.
    %
:- type date_time_format
    --->   mm_dd_yyyy_hh_mm(string, string, string)  % e.g. 03-24-2013 12:23
    ;      dd_mm_yyyy_hh_mm(string, string, string). % e.g. 24-03-2013 12:23

%----------------------------------------------------------------------------%
%
% Convenience functions for common field types.
%

% The following all create field_desc that have no width limit, will cause
% whitespace to be trimmed and will not apply any field actions.

    % NOTE: the field desc returned by this function will not allow floats.
    %
:- func int_field_desc = field_desc.
:- func date_field_desc(date_format) = field_desc.
:- func float_field_desc = field_desc.
:- func floatstr_field_desc = field_desc.
:- func string_field_desc = field_desc.

:- func maybe_date_field_desc(date_format) = field_desc.
:- func maybe_float_field_desc = field_desc.
:- func maybe_floatstr_field_desc = field_desc.
:- func maybe_int_field_desc = field_desc.
:- func maybe_string_field_desc = field_desc.

%----------------------------------------------------------------------------%
%
% CSV data.
%

    % Values of this type represent some CSV data that has been read from a
    % stream and successfully processed.
    %
:- type csv
    --->    csv(
                csv_stream_name :: stream.name,
                csv_header      :: maybe(header),
                csv_records     :: records
            ).

    % Values of this type represent a CSV header.
    %
:- type header
    --->    header(list(string)).

    % Values of this type represent a CSV record.
    %
:- type record
    --->    record(
                record_line_no :: line_number,
                record_fields  :: field_values
            ).

:- type records == list(record).

:- type field_value
    --->    bool(bool)
    ;       int(int)
    ;       float(float)
    ;       floatstr(string)
    ;       string(string)
    ;       date(date)
    ;       date_time(date)
    ;       univ(univ)
    ;       maybe_bool(maybe(bool))
    ;       maybe_int(maybe(int))
    ;       maybe_float(maybe(float))
    ;       maybe_floatstr(maybe(string))
    ;       maybe_string(maybe(string))
    ;       maybe_date(maybe(date))
    ;       maybe_date_time(maybe(date))
    ;       maybe_univ(maybe(univ)).

:- type field_values == list(field_value).

%----------------------------------------------------------------------------%
%
% CSV reader creation and access.
%

    % init_reader(Stream, HeaderDesc, RecordDesc, Reader, !State):
    % Use the default field delimiter.
    %
:- pred init_reader(Stream::in, header_desc::in, record_desc::in,
    reader(Stream)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

:- pred init_reader(Stream::in, reader_params::in, header_desc::in,
    record_desc::in, reader(Stream)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % As above, but use the specified field delimiter character instead
    % of ','.
    %
:- pragma obsolete(init_reader_delimiter/7).
:- pred init_reader_delimiter(Stream::in, header_desc::in, record_desc::in,
    char::in, reader(Stream)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%----------------------------------------------------------------------------%
%
% Initializing readers from the header.
%

% The predicates in this section create CSV readers by reading in the header
% record from some CSV data and then calling a user-defined predicate to
% generate a field descriptor from each header field.  These field descriptors
% are then used when processing the remainder of the CSV data.

:- type init_from_header_result(Stream, Error)
    --->    ok(reader(Stream), header)
    ;       eof
    ;       error(csv.error(Error)).

:- type init_from_header_params
    --->    init_from_header_params(
                init_from_header_record_field_limit :: record_field_limit,
                % What limit, if any, is applied to the number of fields in
                % header?

                init_from_header_field_width_limit :: field_width_limit,
                % What limit, if any, is applied  to the width of fields
                % in the header?

                init_from_header_time_whitespace :: trim_whitespace,
                % Should leading- and trailing-whitespace be stripped from
                % the header fields?

                init_from_header_field_delimiter :: char,
                % The field delimiter character to use.

                init_from_header_ingore_blank_lines :: ignore_blank_lines
                % Should blank lines be ignored?
            ).

    % The following function is provided for backwards compatibility
    % with earlier versions of this library.
    %
:- func init_from_header_params(record_field_limit, field_width_limit,
    trim_whitespace, char) = init_from_header_params.

:- type header_to_field_pred(State)
    == pred(string, line_number, column_number, field_desc, State, State).
:- inst header_to_field_pred == (pred(in, in, in, out, di, uo) is det).

    % init_reader_from_header(Stream, HeaderToField, Result, !State):
    % Initialize a CSV reader from Stream using the predicate HeaderToField
    % to creating a field descriptor corresponding to each header field.
    %
:- pred init_reader_from_header(Stream::in,
    header_to_field_pred(State)::in(header_to_field_pred),
    init_from_header_result(Stream, Error)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % init_reader_from_header(Stream, Params, HeaderToField, Result, !State):
    %
    % As above but use the parameters (see 'CSV raw readers' below) in Params
    % when reading in the the header record.  The CSV reader returned will
    % inherit the field delimiter character from Params.
    %
:- pred init_reader_from_header(Stream::in, init_from_header_params::in,
    header_to_field_pred(State)::in(header_to_field_pred),
    init_from_header_result(Stream, Error)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%
% The '_foldl' versions are similar to the above but have an extra accumulator
% argument threaded through them.
%

:- type header_to_field_foldl_pred(Acc, State) ==
    pred(string, line_number, column_number, field_desc, Acc, Acc, State, State).
:- inst header_to_field_foldl_pred ==
    (pred(in, in, in, out, in, out, di, uo) is det).

:- pred init_reader_from_header_foldl(Stream::in,
    header_to_field_foldl_pred(Acc, State)::in(header_to_field_foldl_pred),
    init_from_header_result(Stream, Error)::out,
    Acc::in, Acc::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

:- pred init_reader_from_header_foldl(Stream::in, init_from_header_params::in,
    header_to_field_foldl_pred(Acc, State)::in(header_to_field_foldl_pred),
    init_from_header_result(Stream, Error)::out,
    Acc::in, Acc::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

%----------------------------------------------------------------------------%
%
% Get / set reader properties.
%
    % Get the underlying stream.
    %
:- func get_stream(reader(Stream)) = Stream
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % Get and set the field delimiter.
    %
:- func get_field_delimiter(reader(Stream)) = char.
:- pred set_field_delimiter(char::in,
    reader(Stream)::in, reader(Stream)::out) is det.

    % Get and set the header descriptor.
    %
:- func get_header_desc(reader(Stream)) = header_desc.
:- pred set_header_desc(header_desc::in,
    reader(Stream)::in, reader(Stream)::out) is det.

    % Get and set the record descriptor.
    %
:- func get_record_desc(reader(Stream)) = record_desc.
:- pred set_record_desc(record_desc::in,
    reader(Stream)::in, reader(Stream)::out) is det.

    % Succeeds iff the given CSV reader has been configured to expect a CSV
    % header.
    %
:- pred has_header(reader(Stream)::in) is semidet.

    % read_from_file(FileName, HeaderDesc, RecordDesc, Result, !IO):
    % Open the text file FileName and read CSV data as per the given header and
    % record descriptors.  The file is closed when EOF is reached.
    %
:- pred read_from_file(string::in, header_desc::in, record_desc::in,
    csv.result(csv, io.error)::out, io::di, io::uo) is det.

%----------------------------------------------------------------------------%
%
% Stream type class instances for CSV readers.
%

% XXX these should be polymorphic in the stream state type, but unfortunately
% restrictions in the type class system mean this is not currently possible.
% The sub-module csv.typed_reader contains predicates that mirror the
% standard stream predicates but which will work for arbitrary stream state.

:- instance stream.error(csv.error(Error)) <= stream.error(Error).

:- instance stream.stream(reader(Stream), io) <= stream.stream(Stream, io).

:- instance stream.input(reader(Stream), io) <= stream.input(Stream, io).

:- instance stream.reader(reader(Stream), record, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.reader(reader(Stream), header, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.reader(reader(Stream), csv, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.line_oriented(reader(Stream), io)
    <= (
        stream.line_oriented(Stream, io)
    ).

%----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%
%
% CSV raw readers.
%

    % A CSV raw reader gets CSV records from an underlying character stream
    % and returns them as lists of strings.
    %
:- type raw_reader(Stream).

:- type raw_reader_params
    --->    raw_reader_params(
                raw_record_field_limit :: record_field_limit,
                raw_field_width_limit  :: field_width_limit,
                raw_field_delimiter    :: char
            ).

:- pred init_raw_reader(Stream::in, raw_reader(Stream)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

:- pred init_raw_reader(Stream::in, raw_reader_params::in,
    raw_reader(Stream)::out, State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

:- pragma obsolete(init_raw_reader/7).
:- pred init_raw_reader(Stream::in, record_field_limit::in,
    field_width_limit::in, char::in, raw_reader(Stream)::out,
    State::di, State::uo) is det
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

:- type raw_record
    --->    raw_record(
                raw_record_line_no :: line_number,
                % The starting line number for this record.

                raw_record_field   :: raw_fields
                % The fields for this record.
            ).

:- type raw_records == list(raw_record).

:- type raw_field
    --->    raw_field(
                raw_field_value   :: string,
                % The value of of this field as a string.

                raw_field_line_no :: line_number,
                % The starting line number for this field.

                raw_field_col_no  :: column_number
                % The starting column number for this field.
            ).

:- type raw_fields == list(raw_field).

:- type raw_csv
    --->    raw_csv(raw_records).

%----------------------------------------------------------------------------%
%
% Stream type class instances for CSV raw readers.
%

% XXX these should be polymorphic in the stream state type, but unfortunately
% restrictions in the type class system mean this is not currently possible.
% The sub-module csv.raw_reader contains predicates that mirror the
% standard stream predicates but which will work for arbitrary stream states.

:- instance stream.stream(raw_reader(Stream), io)
    <= stream.stream(Stream, io).

:- instance stream.input(raw_reader(Stream), io)
    <= stream.input(Stream, io).

:- instance stream.reader(raw_reader(Stream), raw_record, io,
        csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.reader(raw_reader(Stream), raw_csv, io,
        csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.line_oriented(csv.raw_reader(Stream), io)
    <= (
        stream.line_oriented(Stream, io)
    ).

%----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%

:- implementation.

:- include_module csv.char_buffer.
:- include_module csv.record_parser.

:- import_module csv.char_buffer.
:- import_module csv.record_parser.
:- import_module csv.typed_reader.
:- import_module csv.raw_reader.

:- import_module exception.
:- import_module int.
:- import_module list.
:- import_module require.
:- import_module string.

%----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%

:- instance stream.error(csv.error(Error)) <= stream.error(Error) where
[
    func(error_message/1) is make_error_message
].

:- func make_error_message(csv.error(Error)) = string <= stream.error(Error).

make_error_message(Error) = Msg :-
    (
        Error = stream_error(StreamError),
        Msg = stream.error_message(StreamError)
    ;
        Error = csv_error(Name, LineNo, ColNo, FieldNo, CSV_Msg),
        string.format("%s:%d:%d: error: in field #%d, %s",
            [s(Name), i(LineNo), i(ColNo), i(FieldNo), s(CSV_Msg)], Msg)
    ).

%----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%
%
% CSV reader and access procedures.
%

:- type reader(Stream)
    --->    csv_reader(
                csv_stream             :: Stream,
                csv_header_desc        :: header_desc,
                csv_record_desc        :: record_desc,
                csv_field_delimiter    :: char,
                csv_ignore_blank_lines :: ignore_blank_lines,
                % These fields are set directly be the user.

                csv_field_limit  :: record_field_limit,
                csv_width_limit  :: field_width_limit
                % These fields are derived from the above.
            ).

init_reader(Stream, HeaderDesc, RecordDesc, Reader, !State) :-
    Params = reader_params(
        do_not_ignore_blank_lines,
        default_field_delimiter
    ),
    init_reader(Stream, Params, HeaderDesc, RecordDesc, Reader, !State).

init_reader(Stream, Params, HeaderDesc, RecordDesc, Reader, !State) :-
    Params = reader_params(
        IgnoreBlankLines,
        FieldDelimiter
    ),
    list.length(RecordDesc, NumFields),
    FieldLimit = exactly(NumFields),
    WidthLimit = get_max_field_width(HeaderDesc, RecordDesc),
    ( if is_invalid_delimiter(FieldDelimiter)
    then throw(invalid_field_delimiter_error(FieldDelimiter))
    else true
    ),
    Reader = csv_reader(Stream, HeaderDesc, RecordDesc, FieldDelimiter,
        IgnoreBlankLines, FieldLimit, WidthLimit).

init_reader_delimiter(Stream, HeaderDesc, RecordDesc, FieldDelimiter, Reader,
        !State) :-
    Params = reader_params(
        do_not_ignore_blank_lines,
        FieldDelimiter
    ),
    init_reader(Stream, Params, HeaderDesc, RecordDesc, Reader, !State).

%----------------------------------------------------------------------------%

init_from_header_params(RecordFieldLimit, FieldWidthLimit, TrimWhitespace,
        Delimiter) = Params :-
    Params = init_from_header_params(
        RecordFieldLimit,
        FieldWidthLimit,
        TrimWhitespace,
        Delimiter,
        do_not_ignore_blank_lines
    ).

init_reader_from_header(Stream, InitFromHeaderPred, Result, !State) :-
    Params = init_from_header_params(
        no_limit,
        no_limit,
        do_not_trim_whitespace,
        default_field_delimiter,
        do_not_ignore_blank_lines
    ),
    init_reader_from_header(Stream, Params, InitFromHeaderPred, Result,
        !State).

init_reader_from_header(Stream, FromHeaderParams, InitFromHeaderPred,
        Result, !State) :-
    FromHeaderParams = init_from_header_params(
        RecordFieldLimit,
        FieldWidthLimit,
        TrimWhitespace,
        Delimiter,
        IgnoreBlankLines
    ),
    RawParams = raw_reader_params(
        RecordFieldLimit,
        FieldWidthLimit,
        Delimiter
    ),
    init_raw_reader(Stream, RawParams, RawReader, !State),
    get_raw_record(RawReader, RawRecordResult, !State),
    (
        RawRecordResult = ok(RawRecord),
        RawRecord =  raw_record(_HeaderLineNum, RawHeaderFields),
        list.map2_foldl(
            header_field_to_desc(TrimWhitespace, InitFromHeaderPred),
            RawHeaderFields, HeaderFields, FieldDescs, !State),
        ReaderParams = reader_params(
            IgnoreBlankLines,
            Delimiter
        ),
        init_reader(Stream, ReaderParams, no_header, FieldDescs, Reader,
            !State),
        Header = header(HeaderFields),
        Result = ok(Reader, Header)
    ;
        RawRecordResult = eof,
        Result = eof
    ;
        RawRecordResult = error(Error),
        Result = error(Error)
    ).

:- pred header_field_to_desc(trim_whitespace::in,
    header_to_field_pred(State)::in(header_to_field_pred),
    raw_field::in, string::out, field_desc::out, State::di, State::uo) is det.

header_field_to_desc(TrimWhitespace, ToFieldDescPred, RawField, Header,
        FieldDesc, !State) :-
    RawField = raw_field(Header0, LineNumber, ColNumber),
    (
        TrimWhitespace = do_not_trim_whitespace,
        Header = Header0
    ;
        TrimWhitespace = trim_whitespace,
        Header = string.strip(Header0)
    ),
    ToFieldDescPred(Header, LineNumber, ColNumber, FieldDesc, !State).

%----------------------------------------------------------------------------%

init_reader_from_header_foldl(Stream, InitFromHeaderPred, Result, !Acc,
        !State) :-
    Params = init_from_header_params(
        no_limit,
        no_limit,
        do_not_trim_whitespace,
        default_field_delimiter,
        do_not_ignore_blank_lines
    ),
    init_reader_from_header_foldl(Stream, Params, InitFromHeaderPred,
        Result, !Acc, !State).

init_reader_from_header_foldl(Stream, FromHeaderParams, InitFromHeaderPred,
        Result, !Acc, !State) :-
    FromHeaderParams = init_from_header_params(
        RecordFieldLimit,
        FieldWidthLimit,
        TrimWhitespace,
        Delimiter,
        IgnoreBlankLines
    ),
    RawParams = raw_reader_params(
        RecordFieldLimit,
        FieldWidthLimit,
        Delimiter
    ),
    init_raw_reader(Stream, RawParams, RawReader, !State),
    get_raw_record(RawReader, RawRecordResult, !State),
    (
        RawRecordResult = ok(RawRecord),
        RawRecord =  raw_record(_HeaderLineNum, RawHeaderFields),
        list.map2_foldl2(
            header_field_to_desc_foldl(TrimWhitespace, InitFromHeaderPred),
            RawHeaderFields, HeaderFields, FieldDescs, !Acc, !State),
        ReaderParams = reader_params(
            IgnoreBlankLines,
            Delimiter
        ),
        init_reader(Stream, ReaderParams, no_header, FieldDescs, Reader,
            !State),
        Header = header(HeaderFields),
        Result = ok(Reader, Header)
    ;
        RawRecordResult = eof,
        Result = eof
    ;
        RawRecordResult = error(Error),
        Result = error(Error)
    ).

:- pred header_field_to_desc_foldl(trim_whitespace::in,
    header_to_field_foldl_pred(Acc, State)::in(header_to_field_foldl_pred),
    raw_field::in, string::out, field_desc::out,
    Acc::in, Acc::out, State::di, State::uo) is det.

header_field_to_desc_foldl(TrimWhitespace, ToFieldDescPred, RawField,
        Header, FieldDesc, !Acc, !State) :-
    RawField = raw_field(Header0, LineNumber, ColNumber),
    (
        TrimWhitespace = do_not_trim_whitespace,
        Header = Header0
    ;
        TrimWhitespace = trim_whitespace,
        Header = string.strip(Header0)
    ),
    ToFieldDescPred(Header, LineNumber, ColNumber, FieldDesc, !Acc, !State).

%----------------------------------------------------------------------------%

get_stream(Reader) = Reader ^ csv_stream.

get_field_delimiter(Reader) = Reader ^ csv_field_delimiter.

set_field_delimiter(Delimiter, !Reader) :-
    ( if is_invalid_delimiter(Delimiter)
    then throw(invalid_field_delimiter_error(Delimiter))
    else !Reader ^ csv_field_delimiter := Delimiter
    ).

get_header_desc(Reader) = Reader ^ csv_header_desc.

set_header_desc(HeaderDesc, !Reader) :-
    RecordDesc = !.Reader ^ csv_record_desc,
    !Reader ^ csv_header_desc := HeaderDesc,
    !Reader ^ csv_width_limit := get_max_field_width(HeaderDesc, RecordDesc).

get_record_desc(Reader) = Reader ^ csv_record_desc.

set_record_desc(RecordDesc, !Reader) :-
    list.length(RecordDesc, NumFields),
    HeaderDesc = !.Reader ^ csv_header_desc,
    !Reader ^ csv_record_desc := RecordDesc,
    !Reader ^ csv_field_limit := exactly(NumFields),
    !Reader ^ csv_width_limit := get_max_field_width(HeaderDesc, RecordDesc).

:- func get_max_field_width(header_desc, record_desc) = field_width_limit.

get_max_field_width(HeaderDesc, FieldDescs) = MaxWidth :-
    (
        FieldDescs = [],
        (
            HeaderDesc = no_header,
            unexpected($file, $pred, "no fields and no header")
        ;
            % What return here will not matter since the field width limit
            % will change once some fields are actually added.
            HeaderDesc = header_desc(MaxWidth)
        )
    ;
        FieldDescs = [FieldDesc | FieldDescsPrime],
        (
            HeaderDesc = no_header,
            FirstFieldWidth = get_field_width_limit(FieldDesc),
            get_max_field_width_2(FieldDescsPrime, FirstFieldWidth, MaxWidth)
        ;
            HeaderDesc = header_desc(HeaderFieldWidth),
            get_max_field_width_2(FieldDescs, HeaderFieldWidth, MaxWidth)
        )
    ).

:- pred get_max_field_width_2(list(field_desc)::in,
    field_width_limit::in, field_width_limit::out) is det.

get_max_field_width_2([], !MaybeLimit).
get_max_field_width_2([FieldDesc | FieldDescs], !MaybeLimit) :-
    (
        !.MaybeLimit = no_limit
    ;
        !.MaybeLimit = limited(Limit),
        FieldWidthLimit = get_field_width_limit(FieldDesc),
        (
            FieldWidthLimit = no_limit,
            !:MaybeLimit = no_limit
        ;
            FieldWidthLimit = limited(FieldLimit),
            ( if FieldLimit > Limit
            then !:MaybeLimit = FieldWidthLimit
            else true
            ),
            get_max_field_width_2(FieldDescs, !MaybeLimit)
        )
    ).

has_header(Reader) :-
    Reader ^ csv_header_desc = header_desc(_).

:- func get_field_width_limit(field_desc) = field_width_limit.

get_field_width_limit(discard(MaybeLimit)) = MaybeLimit.
get_field_width_limit(field_desc(_, MaybeLimit, _)) = MaybeLimit.

%----------------------------------------------------------------------------%

read_from_file(FileName, HeaderDesc, RecordDesc, Result, !IO) :-
    io.open_input(FileName, OpenFileResult, !IO),
    (
        OpenFileResult = ok(File),
        init_reader(File, HeaderDesc, RecordDesc, Reader, !IO),
        get(Reader, Result, !IO),
        (
            ( Result = ok(_)
            ; Result = eof
            ),
            io.close_input(File, !IO)
        ;
            Result = error(_)
        )
    ;
        OpenFileResult = error(IO_Error),
        Result = error(stream_error(IO_Error))
    ).

%----------------------------------------------------------------------------%

date_field_desc(DateFormat) =
    field_desc(date(DateFormat, []), no_limit, trim_whitespace).

float_field_desc =
    field_desc(float([]), no_limit, trim_whitespace).

floatstr_field_desc =
    field_desc(floatstr([]), no_limit, trim_whitespace).

int_field_desc =
    field_desc(int(do_not_allow_floats, []), no_limit, trim_whitespace).

string_field_desc =
    field_desc(string([]), no_limit, trim_whitespace).

maybe_date_field_desc(DateFormat) =
    field_desc(maybe(date(DateFormat, [])), no_limit, trim_whitespace).

maybe_float_field_desc =
    field_desc(maybe(float([])), no_limit, trim_whitespace).

maybe_floatstr_field_desc =
    field_desc(maybe(floatstr([])), no_limit, trim_whitespace).

maybe_int_field_desc =
    field_desc(maybe(int(do_not_allow_floats, [])), no_limit, trim_whitespace).

maybe_string_field_desc =
    field_desc(maybe(string([])), no_limit, trim_whitespace).

%----------------------------------------------------------------------------%

:- instance stream.stream(csv.reader(Stream), io) <= stream(Stream, io) where
[
    ( name(Reader, Name, !State) :-
        stream.name(Reader ^ csv_stream, Name, !State)
    )
].

:- instance stream.input(reader(Stream), io) <= stream.input(Stream, io)
    where [].

:- instance stream.reader(reader(Stream), header, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    pred(get/4) is csv.typed_reader.get_header
].

:- instance stream.reader(reader(Stream), record, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    pred(get/4) is csv.typed_reader.get_record
].

:- instance stream.reader(reader(Stream), csv.csv, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    pred(get/4) is csv.typed_reader.get_csv
].

:- instance stream.line_oriented(reader(Stream), io)
    <= (
        stream.line_oriented(Stream, io)
    ) where
[
    ( get_line(Reader, LineNo, !IO) :-
        stream.get_line(Reader ^ csv_stream, LineNo, !IO)
    ),

    ( set_line(Reader, LineNo, !IO) :-
        stream.set_line(Reader ^ csv_stream, LineNo, !IO)
    )
].

%----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%

:- type csv.raw_reader(Stream)
    --->    raw_reader(
                csv_raw_stream      :: Stream,
                csv_raw_field_limit :: record_field_limit,
                csv_raw_width_limit :: field_width_limit,
                csv_raw_delimiter   :: char
            ).

%----------------------------------------------------------------------------%

init_raw_reader(Stream, Reader, !State) :-
    Params = raw_reader_params(no_limit, no_limit, default_field_delimiter),
    init_raw_reader(Stream, Params, Reader, !State).

init_raw_reader(Stream, Params, Reader, !State) :-
    Params = raw_reader_params(RecordLimit, FieldWidthLimit, FieldDelimiter),
    ( if is_invalid_delimiter(FieldDelimiter)
    then throw(invalid_field_delimiter_error(FieldDelimiter))
    else true
    ),
    Reader = raw_reader(Stream, RecordLimit, FieldWidthLimit, FieldDelimiter).

init_raw_reader(Stream, RecordLimit, FieldWidthLimit, FieldDelimiter, Reader,
        !State) :-
    ( if is_invalid_delimiter(FieldDelimiter)
    then throw(invalid_field_delimiter_error(FieldDelimiter))
    else true
    ),
    Reader = raw_reader(Stream, RecordLimit, FieldWidthLimit,
        FieldDelimiter).

%----------------------------------------------------------------------------%

:- instance stream.stream(raw_reader(Stream), io)
        <= stream.stream(Stream, io) where
[
    ( name(Reader, Name, !State) :-
        stream.name(Reader ^ csv_raw_stream, Name, !State)
    )
].

:- instance stream.input(raw_reader(Stream), io)
        <= stream.input(Stream, io) where [].

:- instance stream.reader(raw_reader(Stream), raw_record, io,
        csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    pred(get/4) is csv.raw_reader.get_raw_record
].

:- instance stream.reader(raw_reader(Stream), raw_csv, io,
        csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    pred(get/4) is csv.raw_reader.get_raw_csv
].

:- instance stream.line_oriented(raw_reader(Stream), io)
    <= (
        stream.line_oriented(Stream, io)
    ) where
[
    ( get_line(Reader, LineNo, !IO) :-
        stream.get_line(Reader ^ csv_raw_stream, LineNo, !IO)
    ),

    ( set_line(Reader, LineNo, !IO) :-
        stream.set_line(Reader ^ csv_raw_stream, LineNo, !IO)
    )
].

%----------------------------------------------------------------------------%

:- func default_field_delimiter = char.

default_field_delimiter = (',').

is_invalid_delimiter(('"')).
is_invalid_delimiter(('\n')).

%----------------------------------------------------------------------------%
:- end_module csv.
%----------------------------------------------------------------------------%
