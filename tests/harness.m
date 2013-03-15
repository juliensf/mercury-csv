%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%

:- module harness.
:- interface.

:- import_module char.
:- import_module csv.
:- import_module io.
:- import_module stream.

:- pred process_csv(csv.reader(Stream)::in, io.text_output_stream::in,
    csv.res(Error)::out, io::di, io::uo) is det
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module bool.
:- import_module list.
:- import_module require.
:- import_module string.

%-----------------------------------------------------------------------------%

process_csv(Reader, OutFile, Result, !IO) :-
    ( if csv.has_header(Reader) then
        stream.get(Reader, HeaderResult, !IO),
        (
            HeaderResult = ok(Header),
            write_header(OutFile, Header, !IO),
            process_records(Reader, OutFile, Result, !IO)
        ;
            HeaderResult = eof,
            stream.name(Reader, Name, !IO),
            stream.get_line(Reader, LineNo, !IO),
            HeaderError = csv_error(
                Name,
                LineNo,
                1,
                "unexpected EOF in header"
            ),
            Result = error(HeaderError) 
        ;
            HeaderResult = error(HeaderError),
            Result = error(HeaderError)
        )
    else
        process_records(Reader, OutFile, Result, !IO)
    ).

:- pred process_records(csv.reader(Stream)::in, io.text_output_stream::in,
    csv.res(Error)::out, io::di, io::uo) is det
    <= (
        stream.line_oriented(Stream, io),
        stream.putback(Stream, char, io, Error)
    ).

process_records(Reader, OutFile, Result, !IO) :-
    stream.input_stream_fold_state(Reader, write_record(OutFile), Result, !IO).

%-----------------------------------------------------------------------------%

:- pred write_header(io.text_output_stream::in, header::in,
    io::di, io::uo) is det.

write_header(File, Header, !IO) :-
    Header = header(HeaderFields),
    io.write_list(File, HeaderFields, ",", write_quoted_string, !IO),
    io.nl(File, !IO).

:- pred write_quoted_string(string::in, io::di, io::uo) is det.

write_quoted_string(String, !IO) :-
    io.write_char(('"'), !IO),
    io.write_string(String, !IO),
    io.write_char(('"'), !IO).

%-----------------------------------------------------------------------------%

:- pred write_record(io.text_output_stream::in, record::in,
	io::di, io::uo) is det.

write_record(File, Record, !IO) :-
	Record = record(_LineNo, Fields),
    io.write_list(File, Fields, ",", write_field_value, !IO),
    io.nl(File, !IO).

:- pred write_field_value(field_value::in, io::di, io::uo) is det.

write_field_value(Value, !IO) :-
    io.write_char(('"'), !IO),
    (
        Value = bool(Bool),
        io.write(Bool, !IO)
    ;
        Value = int(Int),
        io.write_int(Int, !IO)
    ;
        Value = float(Float),
        io.write_float(Float, !IO)
    ;
        Value = floatstr(FloatStr),
        io.write_string(FloatStr, !IO)
    ;
        Value = string(String),
        io.write_string(String, !IO)
    ;
        Value = date(Date),
        io.write(Date, !IO)
    ;
        Value = term(_Varset, Term),
        io.write(Term, !IO)
    ;
        Value = univ(Univ),
        io.write(Univ, !IO)
    ),
    io.write_char(('"'), !IO).

%-----------------------------------------------------------------------------%
:- end_module harness.
%-----------------------------------------------------------------------------%
