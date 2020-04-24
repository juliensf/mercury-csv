%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
%
% An example of how to initialize a CSV reader from the header fields in the
% CSV data.
%
% CSV data is read from the standard input.
%
% Fields whose header has the prefix "INT_" are treated as integer
% fields.
% Fields whose header has the prefix "FLOAT_" are treated as floats fields.
% Fields whose header has the prefix "STRING_" are treated as strings.
% Fields whose header has the prefix "DATE_" are treated as dates in
% YYYY-MM-DD format.
% Field whose header does not match one of the above are discarded.
%
% After the CSV data is read the records are printed, one per line, and then
% the number of discarded fields is printed.
%
%-----------------------------------------------------------------------------%

:- module init_from_header_ex.
:- interface.

:- import_module io.

:- pred main(io::di, io::uo) is det.

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module csv.

:- import_module int.
:- import_module list.
:- import_module stream.
:- import_module string.

%-----------------------------------------------------------------------------%

main(!IO) :-
    io.stdin_stream(Stdin, !IO),
    FromHeaderParams = init_from_header_params(
        no_limit,
        no_limit,
        trim_whitespace,
        (',')
    ),
    csv.init_reader_from_header_foldl(Stdin, FromHeaderParams, make_field_desc,
        InitReaderResult, 0, NumDiscards, !IO),
    (
        InitReaderResult = ok(Reader, _Header),
        stream.get(Reader, CSVDataResult, !IO),
        (
            CSVDataResult = ok(CSVData),
            io.write_list(CSVData ^ csv_records, "", io.print_line, !IO)
        ;
            CSVDataResult = eof
        ;
            CSVDataResult = error(Error),
            ErrorMsg = stream.error_message(Error),
            io.stderr_stream(Stderr, !IO),
            io.format(Stderr, "error: %s.\n.", [s(ErrorMsg)], !IO),
            io.set_exit_status(1, !IO)
        ),
        ( if NumDiscards = 1 then
            io.write_string("1 field was discarded.\n", !IO)
        else
            io.format("%d fields were discard\n", [i(NumDiscards)], !IO)
        )
    ;
        InitReaderResult = eof,
        io.stderr_stream(Stderr, !IO),
        io.write_string(Stderr, "error: EOF at start of stream.\n", !IO),
        io.set_exit_status(1, !IO)
    ;
        InitReaderResult = error(Error),
        ErrorMsg = stream.error_message(Error),
        io.stderr_stream(Stderr, !IO),
        io.format(Stderr, "error: %s.\n", [s(ErrorMsg)], !IO),
        io.set_exit_status(1, !IO)
    ).

:- pred make_field_desc(string::in, line_number::in, column_number::in,
    field_desc::out, int::in, int::out, io::di, io::uo) is det.

make_field_desc(FieldName, _LineNumber, _ColumnNumber, FieldDesc,
        !NumDiscards, !IO) :-
    ( if string.prefix(FieldName, "INT_") then
        FieldDesc = int_field_desc
    else if string.prefix(FieldName, "FLOAT_") then
        FieldDesc = float_field_desc
    else if string.prefix(FieldName, "STRING_") then
        FieldDesc = string_field_desc
    else if string.prefix(FieldName, "DATE_") then
        FieldDesc = field_desc(date(yyyy_mm_dd("-"), []), no_limit,
            trim_whitespace)
    else
        % Otherwise it's not a field we recognise so just discard it.
        !:NumDiscards = !.NumDiscards + 1,
        FieldDesc = discard(no_limit)
    ).

%-----------------------------------------------------------------------------%
:- end_module init_from_header_ex.
%-----------------------------------------------------------------------------%
