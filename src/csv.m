%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013-2014 Julien Fischer.
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
:- import_module term.
:- import_module varset.
:- import_module univ.

%----------------------------------------------------------------------------%
%
% CSV errors.
%

:- type csv.line_number == int.
:- type csv.column_number == int.
:- type csv.field_number == int.

    % This type describes errors that can occur while processing a CSV
    % stream.
    %
:- type csv.error(Error)
    --->    stream_error(Error)
            % An error has occurred in the underlying character stream.
            
    ;       csv_error(
                csv_err_name  :: stream.name,
                csv_err_line  :: csv.line_number,
                csv_err_col   :: csv.column_number,
                csv_err_field :: csv.field_number,
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
:- pred csv.is_invalid_delimiter(char::in) is semidet.

    % Exceptions of this type are thrown if an invalid field delimiter
    % is passed to one of the reader initialisation functions below.
    %
:- type csv.invalid_field_delimiter_error
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
    % appear in a record.
    %
:- type csv.record_field_limit
    --->    no_limit
    ;       exactly(int).

    % What limit, if any, is imposed on the number of characters that may
    % appear in a field.  This limit is exclusive of the quote characters
    % for quoted fields.
    %
:- type csv.field_width_limit
    --->    no_limit
    ;       limited(int). 

    % Should leading- and trailing-whitespace be trimmed from a field?
    %
:- type csv.trim_whitespace
    --->    trim_whitespace
    ;       do_not_trim_whitespace.

%----------------------------------------------------------------------------%
%
% CSV reader.
%

    % A CSV reader gets CSV records from an underlying character stream,
    % optionally applies some post-processing to the field values and then
    % returns the field values as lists of Mercury data types.
    %
:- type csv.reader(Stream).

    % This type specifies whether the CSV data begins with a header line
    % or not.
    %
:- type csv.header_desc
    --->    no_header
    ;       header_desc(
                header_width_limit :: field_width_limit
            ).

:- type csv.record_desc == list(field_desc).

:- type csv.field_desc
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
            % The "float_int_handler" argument specifies what should be
            % done if a float value is encountered in the field.

    ;       float(field_actions(float))
            % The field is a Mercury float value.

    ;       floatstr(field_actions(string))
            % The field represents a float value, but we return it as Mercury
            % string.
            % NOTE: after applying any user actions the value is check again.
            % An error returned if the string no longer represents a float.

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
    
    ;       term(field_actions({varset, term}))
            % A Mercury ground term and its corresponding varset.

    ;       univ(univ_handler, field_actions(univ))
            % A Mercury univ/0 value.
            % The user provided "univ_handler" function is responsible for
            % converting the field's string representation into a univ.

    ;       maybe(field_type).
            % A Mercury maybe/0 value.
            % A blank field in the CSV data corresponds to maybe.no/0.
    
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
    --->   mm_dd_yyyy_hh_mm(string, string, string). % e.g. 03-24-2013 12:23

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
:- type csv.header
    --->    header(list(string)).

    % Values of this type represent a CSV record.
    %
:- type csv.record
    --->    record(
                record_line_no :: line_number,
                record_fields  :: field_values
            ).

:- type csv.records == list(record).

:- type csv.field_value 
    --->    bool(bool) 
    ;       int(int)
    ;       float(float)
    ;       floatstr(string)
    ;       string(string)
    ;       date(date)
    ;       date_time(date)
    ;       term(varset, term)
    ;       univ(univ)
    ;       maybe_bool(maybe(bool))
    ;       maybe_int(maybe(int))
    ;       maybe_float(maybe(float))
    ;       maybe_floatstr(maybe(string))
    ;       maybe_string(maybe(string))
    ;       maybe_date(maybe(date))
    ;       maybe_date_time(maybe(date))
    ;       maybe_term(maybe({varset, term}))
    ;       maybe_univ(maybe(univ)).

:- type csv.field_values == list(csv.field_value).

%----------------------------------------------------------------------------%
%
% CSV reader creation and access.
%

    % csv.init_reader(Stream, HeaderDesc, RecordDesc) = Reader:
    % Use the default field delimiter.
    %
:- func csv.init_reader(Stream, header_desc, record_desc)
    = csv.reader(Stream)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % As above, but use the specified field delimiter character instead
    % of ','.
    %
:- func csv.init_reader_delimiter(Stream, header_desc, record_desc, char)
    = csv.reader(Stream)
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % Get the underlying stream.
    %
:- func get_stream(csv.reader(Stream)) = Stream
    <= (
        stream.line_oriented(Stream, State),
        stream.putback(Stream, char, State, Error)
    ).

    % Get and set the field delimiter.
    %
:- func get_field_delimiter(csv.reader(Stream)) = char.
:- pred set_field_delimiter(char::in,
    csv.reader(Stream)::in, csv.reader(Stream)::out) is det.

    % Get and set the header descriptor.
    %
:- func get_header_desc(csv.reader(Stream)) = header_desc.
:- pred set_header_desc(header_desc::in,
    csv.reader(Stream)::in, csv.reader(Stream)::out) is det.

    % Get and set the record descriptor.
    %
:- func get_record_desc(csv.reader(Stream)) = record_desc.
:- pred set_record_desc(record_desc::in,
    csv.reader(Stream)::in, csv.reader(Stream)::out) is det.

    % Succeeds iff the given CSV reader has been configured to expect a CSV
    % header.
    %
:- pred has_header(csv.reader(Stream)::in) is semidet.

%----------------------------------------------------------------------------%
% 
% Stream type class instances for CSV readers.
%

% XXX these should be polymorphic in the stream state type, but unfortunately
% restrictions in the type class system mean this is not currently possible.
% The sub-module csv.typed_reader contains predicates that mirror the
% standard stream predicates but which will work for arbitrary stream state.

:- instance stream.error(csv.error(Error)) <= stream.error(Error).

:- instance stream.stream(csv.reader(Stream), io) <= stream.stream(Stream, io).

:- instance stream.input(csv.reader(Stream), io) <= stream.input(Stream, io).

:- instance stream.reader(csv.reader(Stream), csv.record, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.reader(csv.reader(Stream), csv.header, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.reader(csv.reader(Stream), csv.csv, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- instance stream.line_oriented(csv.reader(Stream), io)
    <= (
        stream.line_oriented(Stream, io)
    ).

%----------------------------------------------------------------------------%
%----------------------------------------------------------------------------%
%
% CSV raw reader.
%

    % A CSV raw reader gets CSV records from an underlying character stream
    % and returns them as lists of strings.
    %
:- type csv.raw_reader(Stream).

:- func csv.init_raw_reader(Stream) = csv.raw_reader(Stream)
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- func csv.init_raw_reader(Stream, record_field_limit,
    field_width_limit, char) = csv.raw_reader(Stream)
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

:- type csv.raw_record
    --->    raw_record(
                raw_record_line_no :: int,
                % The starting line number for this record.

                raw_record_field   :: raw_fields
                % The fields for this record.
            ).

:- type csv.raw_field
    --->    raw_field(
                raw_field_value   :: string,
                % The value of of this field as a string.

                raw_field_line_no :: int,
                % The starting line number for this field.

                raw_field_col_no  :: int
                % The starting column number for this field.
            ).

:- type csv.raw_fields == list(csv.raw_field).

%----------------------------------------------------------------------------%
% 
% Stream type class instances for CSV raw readers.
%

% XXX these should be polymorphic in the stream state type, but unfortunately
% restrictions in the type class system mean this is not currently possible.
% The sub-module csv.raw_reader contains predicates that mirror the
% standard stream predicates but which will work for arbitrary stream state.

:- instance stream.stream(csv.raw_reader(Stream), io)
    <= stream.stream(Stream, io).

:- instance stream.input(csv.raw_reader(Stream), io)
    <= stream.input(Stream, io).

:- instance stream.reader(csv.raw_reader(Stream), csv.raw_record, io,
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

:- type csv.reader(Stream)
    --->    csv_reader(
                csv_stream          :: Stream,
                csv_header_desc     :: header_desc,
                csv_record_desc     :: record_desc,
                csv_field_delimiter :: char,
                % These fields are set directly be the user.
                
                csv_field_limit  :: record_field_limit,
                csv_width_limit  :: field_width_limit
                % These fields are derived from the above.
            ).

init_reader(Stream, HeaderDesc, RecordDesc) = 
    init_reader_delimiter(Stream, HeaderDesc, RecordDesc,
        default_field_delimiter).

init_reader_delimiter(Stream, HeaderDesc, RecordDesc, FieldDelimiter) 
        = Reader :-
    list.length(RecordDesc, NumFields),
    FieldLimit = exactly(NumFields),
    WidthLimit = get_max_field_width(HeaderDesc, RecordDesc),
    ( if is_invalid_delimiter(FieldDelimiter)
    then throw(invalid_field_delimiter_error(FieldDelimiter))
    else true
    ),
    Reader = csv_reader(Stream, HeaderDesc, RecordDesc, FieldDelimiter,
        FieldLimit, WidthLimit).

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

:- instance stream.stream(csv.reader(Stream), io) <= stream(Stream, io) where
[
    ( name(Reader, Name, !State) :-
        stream.name(Reader ^ csv_stream, Name, !State)
    )
].

:- instance stream.input(csv.reader(Stream), io) <= stream.input(Stream, io)
    where [].

:- instance stream.reader(csv.reader(Stream), header, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    ( get(Reader, Result, !State) :-
        csv.typed_reader.get_header(Reader, Result, !State)
    )
].

:- instance stream.reader(csv.reader(Stream), record, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    ( get(Reader, Result, !State) :-
        csv.typed_reader.get_record(Reader, Result, !State)
    )
].

:- instance stream.reader(csv.reader(Stream), csv.csv, io, csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    ( get(Reader, Result, !State) :-
        csv.typed_reader.get_csv(Reader, Result, !State)
    )
].

:- instance stream.line_oriented(csv.reader(Stream), io)
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
                
init_raw_reader(Stream) = 
    init_raw_reader(Stream, no_limit, no_limit, default_field_delimiter).

init_raw_reader(Stream, RecordLimit, FieldWidthLimit, FieldDelimiter) =
        Reader :-
    ( if is_invalid_delimiter(FieldDelimiter)
    then throw(invalid_field_delimiter_error(FieldDelimiter))
    else true
    ),
    Reader = raw_reader(Stream, RecordLimit, FieldWidthLimit,
        FieldDelimiter).

%----------------------------------------------------------------------------%

:- instance stream.stream(csv.raw_reader(Stream), io)
        <= stream.stream(Stream, io) where
[
    ( name(Reader, Name, !State) :-
        stream.name(Reader ^ csv_raw_stream, Name, !State)
    )
].

:- instance stream.input(csv.raw_reader(Stream), io)
        <= stream.input(Stream, io) where [].

:- instance stream.reader(csv.raw_reader(Stream), csv.raw_record, io,
        csv.error(Error))
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ) where
[
    ( get(Reader, Result, !State) :-
        csv.raw_reader.get_raw_record(Reader, Result, !State)
    )   
].

:- instance stream.line_oriented(csv.raw_reader(Stream), io)
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
