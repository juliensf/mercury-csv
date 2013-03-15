%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
%
% Author: Julien Fischer <juliensf@gmail.com>
%
% A small example of how to use the CSV reader.
% 
%-----------------------------------------------------------------------------%

:- module csv_ex.
:- interface.

%-----------------------------------------------------------------------------%

:- import_module io.

:- pred main(io::di, io::uo) is det.

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module csv.

:- import_module bool.
:- import_module int.
:- import_module list.
:- import_module maybe.
:- import_module require.
:- import_module stream.

%-----------------------------------------------------------------------------%

main(!IO) :-
    io.command_line_arguments(Args, !IO),
    (
        % If there are no command line arguments then read our input
        % from the standard input.
        %
        Args = [],
        io.stdin_stream(InputFile, !IO),
        process_csv(InputFile, !IO)
    ;
        Args = [InputFileName],
        io.open_input(InputFileName, OpenInputResult, !IO),
        (
            OpenInputResult = ok(InputFile),
            process_csv(InputFile, !IO),
            io.close_input(InputFile, !IO)
        ;
            OpenInputResult = error(_IO_Error),
            error("cannot open input file")
        )
    ;
        Args = [_, _ | _],
        error("multiple input arguments")
    ).

%-----------------------------------------------------------------------------%

:- pred process_csv(io.text_input_stream::in, io::di, io::uo) is det.

process_csv(InputFile, !IO) :-

    % A CSV header descriptor (type: csv.header_desc/0) tells the CSV reader
    % whether we expect the file to have a header line and, if so, is there
    % any field width limit for the header fields? 
    %
    % Here we expect the presence of an initial header line and limit the
    % width of the header fields to at most 255 characters.
    %
    HeaderDesc = csv.header_desc(limited(255)), 
   
    % A CSV record descriptor (type: csv.record_desc/0) describes the structure
    % of a record (i.e. a single line) in the CSV data.  It is a list of
    % field descriptors (type: csv.field_desc/0), where the order of the elements
    % in the list corresponds to the order of the fields in the record.
    %
    RecordDesc = [ 

        % This field descriptor tells the CSV reader to expect the first field
        % to be an integer.  The "do_not_allow_floats" parameter tells the
        % reader not to allow a floating point value in place of an int.
        % (Using this parameter you can tell the reader to accept a floating
        % point value in place of an int, so long as you provide a method
        % for converting the floating point value to an int.
        % The user-supplied "field action" function ensure_gt_zero/1 will be
        % called with the field value as its argument after the field has been
        % read in (see below for the definition of ensure_gt_zero/1).
        %
        % The second argument of this field descriptor, "limited(10)", says
        % that this maximum number of characters that can occur in this field
        % is 10 characters.  The CSV reader will return an error if this limit
        % is exceeded.
        %
        % The third argument, "do_not_trim_whitespace", tells the CSV reader
        % not to trim any leading- or trailing-whitespace from the raw field
        % value (i.e. the character data we read in) before attempting to
        % convert it into an int.
        %
        field_desc(int(do_not_allow_floats, [ensure_gt_zero]),
            limited(10), do_not_trim_whitespace),

        % The first argument of this field descriptor says that this field
        % represents a date.  The format of the date is DD-B-YYYY, where
        % DD is the day of the month as an integer, B is the abbreviated
        % name of the month (e.g. Jan, Feb etc) and YYYY is the year as
        % an integer.  The components of the data are expected to be separated
        % by the string "-".
        % There are no user-supplied actions associated with this field.
        %
        % The second argument limits the number of characters in the field
        % to 11 characters, whilst the third argument says that we *should*
        % trim leading- and trailing-whitespace from this field before
        % attempting to convert into a date.
        %
        % NOTE: the CSV reader will return an error if the date is not
        % actually a valid date, e.g. February 29 2013.
        %
        field_desc(date(dd_b_yyyy("-"), []), limited(11), trim_whitespace),

        % XXX TODO - document me.
        %
        field_desc(string([]), no_limit, do_not_trim_whitespace),
        
        % XXX TODO - document me.
        %
        field_desc(float([]), limited(20), do_not_trim_whitespace)
    ],

    % Create the CSV reader.
    %
    Reader = csv.init_reader(InputFile, HeaderDesc, RecordDesc), 

    % Read in the entire CSV file.
    % Since CSV readers are instances of the standard library's stream
    % type classes we can just use the generic stream.get/4 method to do
    % this.
    %
    stream.get(Reader, MaybeData, !IO),
    (
        MaybeData = ok(Data),
        Data = csv(_StreamName, Header, Records),

        % Print out the CSV data we have just read.
        %
        io.write_string("Header: ", !IO),
        io.write(Header, !IO),
        io.nl(!IO),
        io.write_list(Records, "\n", io.write, !IO),
        io.nl(!IO)
    ;
        MaybeData = eof
    ;
        MaybeData = error(_)
    ).

%-----------------------------------------------------------------------------%

:- func ensure_gt_zero(int) = field_action_result(int).

ensure_gt_zero(N) = Result :-
    ( if N > 0
    then Result = ok(N)
    else Result = error("less than or equal to zero")
    ).

%-----------------------------------------------------------------------------%
:- end_module  csv_ex.
%-----------------------------------------------------------------------------%
