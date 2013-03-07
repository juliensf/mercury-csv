%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%

:- module test_csv.
:- interface.

:- import_module io.
:- pred main(io::di, io::uo) is det.

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module csv.
:- import_module harness.
:- import_module test_cases.

:- import_module bool.
:- import_module char.
:- import_module getopt_io.
:- import_module int.
:- import_module list.
:- import_module maybe.
:- import_module set.
:- import_module string.
:- import_module stream.

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

main(!IO) :-
    io.command_line_arguments(Args, !IO),
    OptionOps = option_ops_multi(
        (pred(C::in, O::out) is semidet :- short_option(C, O)),
        long_option,
        (pred(O::out, D::out) is multi :- option_defaults(O, D))
    ),
    getopt_io.process_options(OptionOps, Args, NonOptionArgs, OptionResult,
        !IO),
    (
        OptionResult = ok(OptionTable),
        getopt_io.lookup_bool_option(OptionTable, help, Help),
        (
            Help = yes,
            help(!IO)
        ;
            Help = no,
            (
                NonOptionArgs = [],
                TestsToRun = tests
            ;
                NonOptionArgs = [_ | _],
                list.filter(test_name_matches(NonOptionArgs), tests, TestsToRun)
            ),
            Results0 = init_test_results,
            list.length(TestsToRun, NumTests),
            list.foldl3(run_test(OptionTable, NumTests), TestsToRun, 1, _,
                Results0, Results, !IO),
            print_results(Results, !IO)
        )
    ;
        OptionResult = error(Msg),
        bad_cmdline(Msg, !IO)
    ).

:- pred test_name_matches(list(string)::in, test_case::in) is semidet.

test_name_matches(TestNames, TestCase) :-
    Name = TestCase ^ test_name,
    list.member(Name, TestNames).

%-----------------------------------------------------------------------------%

:- pred run_test(option_table(option)::in, int::in, test_case::in,
    int::in, int::out, test_results::in, test_results::out, io::di, io::uo)
    is det.

run_test(OptionTable, _TotalNumTests, TestCase, !TestNum, !Results, !IO) :-
    increment_total_tests(!Results),
    TestCase = test_case(Type, Name, HeaderDesc, RecordDesc),
    maybe_verbose(OptionTable, io.format("RUNNING TEST: %s ... ", [s(Name)]),
        !IO),
    (
        Type = test_type_valid,
        run_test_valid(OptionTable, Name, HeaderDesc, RecordDesc, !Results,
            !IO)
    ;
        Type = test_type_invalid,
        run_test_invalid(OptionTable, Name, HeaderDesc, RecordDesc, !Results,
            !IO)
    ),
    !:TestNum = !.TestNum + 1.

%-----------------------------------------------------------------------------%
%
% "Valid" tests are those where we expect the CSV reader to produce some
% CSV data.  We then write out that data and diff the result against an
% expected output.

:- pred run_test_valid(option_table(option)::in,
    string::in, header_desc::in, record_desc::in,
    test_results::in,  test_results::out,  io::di, io::uo) is det.

run_test_valid(OptionTable, Name, HeaderDesc, RecordDesc,
        !Results, !IO) :-
    InputFileName = Name ++ ".inp",
    io.open_input(InputFileName, InputResult, !IO),
    (
        InputResult = ok(InputFile),
        OutputFileName = Name ++ ".out",
        io.open_output(OutputFileName, OutputResult, !IO),
        (
            OutputResult = ok(OutputFile),
            Reader = init_reader(InputFile, HeaderDesc, RecordDesc),
            process_csv(Reader, OutputFile, Result, !IO),
            (
                Result = ok,
                io.close_input(InputFile, !IO),
                io.close_output(OutputFile, !IO),
                ExpFileName = Name ++ ".exp",
                ResFileName = Name ++ ".res",
                string.format("diff -u %s %s > %s",
                    [s(ExpFileName), s(OutputFileName), s(ResFileName)],
                    DiffCmd),
                io.call_system(DiffCmd, DiffCmdRes, !IO),
                (
                    DiffCmdRes = ok(DiffExitStatus),
                    ( if DiffExitStatus = 0 then
                        maybe_verbose(OptionTable, 
                            io.write_string("PASSED\n"), !IO),
                        lookup_bool_option(OptionTable, keep_files, KeepFiles),
                        (
                            KeepFiles = yes
                        ;
                            KeepFiles = no,
                            io.remove_file(ResFileName, _, !IO),
                            io.remove_file(OutputFileName, _, !IO)
                        )
                    else
                        add_failed_test(Name, !Results),
                        maybe_verbose(OptionTable,
                            io.write_string("FAILED (expected output does not match)\n"), !IO)
                    ) 
                ;
                    DiffCmdRes = error(DiffError),
                    add_aborted_test(Name, !Results),
                    io.error_message(DiffError, Msg),
                    maybe_verbose(OptionTable,
                        io.format("ABORTED (diff: %s)\n",
                            [s(Msg)]), !IO)
                )
            ;
                Result = error(ProcessCSV_Error),
                (
                    ProcessCSV_Error = stream_error(StreamError),
                    add_aborted_test(Name, !Results),
                    Msg = stream.error_message(StreamError),
                    maybe_verbose(OptionTable,
                        io.format("ABORTED (stream error: %s)\n",
                            [s(Msg)]), !IO)
                ;
                    ProcessCSV_Error = csv_error(StreamName, LineNo, FieldNo, Msg),
                    ErrFileName = Name ++ ".err",
                    io.open_output(ErrFileName, ErrFileResult, !IO),
                    (
                        ErrFileResult = ok(ErrFile),
                        write_csv_error(ErrFile, StreamName, LineNo, FieldNo, Msg, !IO),
                        io.close_output(ErrFile, !IO),
                        add_failed_test(Name, !Results),
                        maybe_verbose(OptionTable,
                            io.write_string("FAILED (CSV parse error)\n"), !IO)
                    ;
                        ErrFileResult = error(IO_Error),
                        add_aborted_test(Name, !Results),
                        report_file_open_error(ErrFileName, IO_Error, !IO)
                    )
                )
            )
        ;
            OutputResult = error(IO_Error),
            add_aborted_test(Name, !Results),
            report_file_open_error(OutputFileName, IO_Error, !IO)
        )
    ;
        InputResult = error(IO_Error),
        add_aborted_test(Name, !Results),
        report_file_open_error(InputFileName, IO_Error, !IO)
    ).

%-----------------------------------------------------------------------------%

:- pred run_test_invalid(option_table(option)::in,
    string::in, header_desc::in, record_desc::in,
    test_results::in,  test_results::out,  io::di, io::uo) is det.
    
run_test_invalid(OptionTable, Name, HeaderDesc, RecordDesc,
        !Results, !IO) :-
    InputFileName = Name ++ ".inp",
    io.open_input(InputFileName, InputResult, !IO),
    (
        InputResult = ok(InputFile),
        OutputFileName = Name ++ ".out",
        io.open_output(OutputFileName, OutputResult, !IO),
        (
            OutputResult = ok(OutputFile),
            Reader = init_reader(InputFile, HeaderDesc, RecordDesc),
            process_csv(Reader, OutputFile, Result, !IO),
            (
                Result = error(ProcessCSV_Error),
                (
                    ProcessCSV_Error = stream_error(StreamError),
                    add_aborted_test(Name, !Results),
                    Msg = stream.error_message(StreamError),
                    maybe_verbose(OptionTable,
                        io.format("ABORTED (stream error: %s)\n",
                            [s(Msg)]), !IO)
                ;
                    ProcessCSV_Error = csv_error(StreamName, LineNo, FieldNo, Msg),
                    ErrFileName = Name ++ ".err",
                    io.open_output(ErrFileName, ErrFileResult, !IO),
                    (
                        ErrFileResult = ok(ErrFile),
                        write_csv_error(ErrFile, StreamName, LineNo, FieldNo, Msg, !IO),
                        io.close_output(ErrFile, !IO),
                        ExpErrFileName = Name ++ ".err_exp",
                        ResFileName = Name ++ ".res",
                        string.format("diff -u %s %s > %s",
                            [s(ExpErrFileName), s(ErrFileName), s(ResFileName)],
                            DiffCmd),
                        io.call_system(DiffCmd, DiffCmdRes, !IO),
                        (
                            DiffCmdRes = ok(DiffExitStatus),
                            ( if DiffExitStatus = 0 then
                                maybe_verbose(OptionTable, 
                                    io.write_string("PASSED\n"), !IO),
                                lookup_bool_option(OptionTable, keep_files, KeepFiles),
                                (
                                    KeepFiles = yes
                                ;
                                    KeepFiles = no,
                                    io.remove_file(ResFileName, _, !IO),
                                    io.remove_file(OutputFileName, _, !IO),
                                    io.remove_file(ErrFileName, _, !IO)
                                )
                            else
                                add_failed_test(Name, !Results),
                                maybe_verbose(OptionTable,
                                    io.write_string(
                                    "FAILED (expected error output does not match)\n"), !IO)
                            ) 
                        ;
                            DiffCmdRes = error(DiffError),
                            add_aborted_test(Name, !Results),
                            io.error_message(DiffError, DiffErrMsg),
                            maybe_verbose(OptionTable,
                                io.format("ABORTED (diff: %s)\n",
                                    [s(DiffErrMsg)]), !IO)
                        ) 

                    ;
                        ErrFileResult = error(IO_Error),
                        add_aborted_test(Name, !Results),
                        report_file_open_error(ErrFileName, IO_Error, !IO)
                    )
                )
            ;
                Result = ok,
                add_failed_test(Name, !Results),
                maybe_verbose(OptionTable,
                    io.write_string(
                    "FAILED (erroneously processed successfully)\n"), !IO)
            )
        ;
            OutputResult = error(IO_Error),
            add_aborted_test(Name, !Results),
            report_file_open_error(OutputFileName, IO_Error, !IO)
        )
    ;
        InputResult = error(IO_Error),
        add_aborted_test(Name, !Results),
        report_file_open_error(InputFileName, IO_Error, !IO)
    ).

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%
%
% Test resuts.
%

:- type test_results
    --->    test_results(
                total_tests  :: int,

                failed_tests :: list(string),
                % Tests that failed.

                aborted_tests :: list(string)
                % Tests that caused an error.
            ).

:- func init_test_results = test_results.

init_test_results = test_results(0, [], []).

:- pred increment_total_tests(test_results::in, test_results::out) is det.

increment_total_tests(!Results) :-
    !Results ^ total_tests := !.Results ^ total_tests + 1.

:- pred add_failed_test(string::in,
    test_results::in, test_results::out) is det.

add_failed_test(Name, !Results) :-
    !Results ^ failed_tests := [Name | !.Results ^ failed_tests].

:- pred add_aborted_test(string::in,
    test_results::in, test_results::out) is det.

add_aborted_test(Name, !Results) :-
    !Results ^ aborted_tests := [Name | !.Results ^ aborted_tests].

:- pred print_results(test_results::in, io::di, io::uo) is det.

print_results(Results, !IO) :-
    Results = test_results(_Total, Failed, Aborted),
    list.length(Failed, NumFailed),
    list.length(Aborted, NumAborted),
    ( if NumFailed = 0, NumAborted = 0 then
        io.write_string("ALL TESTS PASSED\n", !IO)
    else 
        ( if NumFailed > 0 then
            io.write_string("SOME TESTS FAILED\n", !IO),
            write_tests_to_file(Failed, "FAILED_TESTS", !IO)
        else
            true
        ),
        ( if NumAborted > 0 then
            io.write_string("SOME TESTS ABORTED\n", !IO),
            write_tests_to_file(Aborted, "ABORTED_TESTS", !IO),
            io.set_exit_status(1, !IO)
        else
            true
        )
    ).

:- pred write_tests_to_file(list(string)::in, string::in,
    io::di, io::uo) is det.

write_tests_to_file(Tests, FileName, !IO) :-
    io.open_output(FileName, OpenResult, !IO),
    (
        OpenResult = ok(File),
        io.write_list(File, Tests, "\n", io.write_string, !IO),
        io.nl(File, !IO)
    ;
        OpenResult = error(IO_Error),
        report_file_open_error(FileName, IO_Error, !IO)
    ).

%-----------------------------------------------------------------------------%

:- pred maybe_verbose(option_table(option)::in,
    pred(io, io)::in(pred(di, uo) is det), io::di, io::uo) is det.

maybe_verbose(OptionTable, Pred, !IO) :-
    getopt_io.lookup_bool_option(OptionTable, verbose, Verbose),
    (
        Verbose = yes,
        Pred(!IO)
    ;
        Verbose = no
    ).

:- pred maybe_not_verbose(option_table(option)::in,
    pred(io, io)::in(pred(di, uo) is det), io::di, io::uo) is det.

maybe_not_verbose(OptionTable, Pred, !IO) :-
    getopt_io.lookup_bool_option(OptionTable, verbose, Verbose),
    (
        Verbose = no,
        Pred(!IO)
    ;
        Verbose = yes
    ).

%-----------------------------------------------------------------------------%
%
% Command line options.
%

:- type option
    --->    help
    ;       verbose
    ;       keep_files.

:- pred short_option(char, option).
:- mode short_option(in, out) is semidet.
:- mode short_option(out, in) is det.

short_option('h', help).
short_option('v', verbose).
short_option('k', keep_files).

:- pred long_option(string::in, option::out) is semidet.

long_option("help", help).
long_option("verbose", verbose).
long_option("keep-files", keep_files).

:- pred option_defaults(option, option_data).
:- mode option_defaults(in, out) is det.
:- mode option_defaults(out, out) is multi.

option_defaults(help, bool(no)).
option_defaults(verbose, bool(no)).
option_defaults(keep_files, bool(no)).

%-----------------------------------------------------------------------------%

:- pred bad_cmdline(string::in, io::di, io::uo) is det.

bad_cmdline(Msg, !IO) :-
    io.stderr_stream(Stderr, !IO),
    io.format(Stderr, "test_csv: %s\n", [s(Msg)], !IO),
    io.write_string(Stderr, "test_csv: use --help for more information.\n",
        !IO),
    io.set_exit_status(1, !IO).

%-----------------------------------------------------------------------------%

:- pred write_csv_error(io.text_output_stream::in, string::in,
    int::in, int::in, string::in, io::di, io::uo) is det.

write_csv_error(File, Name, LineNo, FieldNo, Msg, !IO) :-
    io.format(File, "%s:%d: error: in field #%d, %s\n",
        [s(Name), i(LineNo), i(FieldNo), s(Msg)], !IO).
            
%-----------------------------------------------------------------------------%

:- pred help(io::di, io::uo) is det.

help(!IO) :-
    io.write_string("Usage: test_csv [<options>] [test-case ...]\n", !IO),
    io.write_strings([
        "Options:\n",
        "\t-h, --help\n",
        "\t\tPrint this message.\n",
        "\t-v, -verbose\n",
        "\t\tOutput progress information.\n",
        "\t-k, --keep-files\n",
        "\t\tDo not delete files generated during a test run.\n"
    ], !IO).

%-----------------------------------------------------------------------------%

:- pred report_file_open_error(string::in, io.error::in,
    io::di, io::uo) is det.

report_file_open_error(FileName, Error, !IO) :-
    io.stderr_stream(Stderr, !IO),
    Msg = io.error_message(Error),
    io.format(Stderr, "error opening file `%s': %s\n",
        [s(FileName), s(Msg)], !IO),
    io.set_exit_status(1, !IO).

%-----------------------------------------------------------------------------%
:- end_module test_csv.
%-----------------------------------------------------------------------------%
