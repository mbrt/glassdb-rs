------------------------------ MODULE Common ------------------------------
EXTENDS FiniteSets, Naturals, Sequences

SetOfSequence(sequence) ==
    {sequence[index] : index \in 1..Len(sequence)}

NoDuplicates(sequence) ==
    Cardinality(SetOfSequence(sequence)) = Len(sequence)

RECURSIVE Position(_, _)

Position(sequence, value) ==
    IF Len(sequence) = 0
    THEN 0
    ELSE IF Head(sequence) = value
         THEN 1
         ELSE LET suffix_position == Position(Tail(sequence), value)
              IN IF suffix_position = 0 THEN 0 ELSE suffix_position + 1

PrefixBefore(sequence, value) ==
    LET position == Position(sequence, value)
    IN IF position <= 1
       THEN <<>>
       ELSE SubSeq(sequence, 1, position - 1)

=============================================================================
