%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%
% Copyright (C) 2013-2017, 2022 Julien Fischer.
% See the file COPYING for license details.
%-----------------------------------------------------------------------------%

:- module csv.char_buffer.
:- interface.

%-----------------------------------------------------------------------------%

:- type char_buffer.

:- pred init(char_buffer::out, S::di, S::uo) is det.

:- pred add(char_buffer::in, char::in, S::di, S::uo) is det.

:- func num_chars(char_buffer::in, S::ui) = (int::out) is det.

:- func to_string(char_buffer::in, S::ui) = (string::out) is det.

:- pred chomp_cr(char_buffer::in, S::di, S::uo) is det.

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module mutvar.

%-----------------------------------------------------------------------------%

:- type char_buffer == mutvar(char_buffer_rep).

    % XXX this could be much more efficient if we implemented it directly
    % as code in the target language.
    % NOTE: if we do so, we should use the maximum field width, if available
    % to initialise the buffer.
    %
:- type char_buffer_rep
    --->   char_buffer_rep(list(char), int).

init(Buffer, !State) :-
    promise_pure (
        BufferRep = char_buffer_rep([], 0),
        impure new_mutvar(BufferRep, Buffer),
        !:State = !.State
    ).

add(Buffer, Char, !State) :-
    promise_pure (
        impure get_mutvar(Buffer, BufferRep0),
        BufferRep0 = char_buffer_rep(Chars, NumChars),
        BufferRep = char_buffer_rep([Char | Chars], NumChars + 1),
        impure set_mutvar(Buffer, BufferRep),
        !:State = !.State
    ).

num_chars(Buffer, _State) = NumChars :-
    promise_pure (
        impure get_mutvar(Buffer, BufferRep),
        BufferRep = char_buffer_rep(_, NumChars)
    ).

to_string(Buffer, _State) = String :-
    promise_pure (
        impure get_mutvar(Buffer, BufferRep),
        BufferRep = char_buffer_rep(RevChars, _),
        String = string.from_rev_char_list(RevChars)
    ).

chomp_cr(Buffer, !State) :-
    promise_pure (
        impure get_mutvar(Buffer, BufferRep0),
        BufferRep0 = char_buffer_rep(Chars, NumChars),
        (
            Chars = []
        ;
            Chars = [LastChar | RestChars],
            ( if LastChar = ('\r') then
                BufferRep = char_buffer_rep(RestChars, NumChars - 1),
                impure set_mutvar(Buffer, BufferRep)
            else
                true
            )
        ),
        !:State = !.State
    ).

%-----------------------------------------------------------------------------%
:- end_module char_buffer.
%-----------------------------------------------------------------------------%
