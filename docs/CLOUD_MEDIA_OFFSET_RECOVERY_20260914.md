# September 14 cloud media recovery

The cloud worker stopped at 00:47:48 EDT with MediaError while preparing
`cloudjob_e6d83121a81787752c670c1d4ad91f93`,
`making_new_character_on_fallout_4 (1)`.

The source video container lasts 30.010295 seconds. Its single AAC stream starts
at 2.007007 seconds and lasts 28.003288 seconds. The original preparation produced
28.003250 seconds of PCM, omitting the leading timeline gap. This differs from the
planned 30,010 ms by 2,007 ms, exceeding the existing 2,000 ms tolerance by 7 ms.
The initial decode was retained as an unreceipted WAV. Source hashing and a fresh
strict decode succeeded; no storage failure was observed for this recording.

`pipeline/cloud_audio_offset_repair.py` is restricted to this exact job and source
hash. Under the cloud workspace lock it refuses paid or already-receipted targets,
verifies the source and decoder, compares all retained PCM with a fresh decode,
and prepends 32,112 zero-valued samples to restore the video timeline. It does not
change speech samples, relax duration validation, change the immutable runtime,
or modify other jobs. Corrected duration is 30,010 ms. Normal preparation receipt
replay passed. The original decode and a provenance report remain in the job's
`start-offset-repair-v1/` directory. No file was discarded.

The existing cloud transcription service was restarted with its original release,
plan, USD150 cap, and durable submission records; paid requests are not replayed.
The Gemini service was not restarted or edited. This is a targeted repair, not a
general deployment of offset handling to every future input. Other genuine
duration discrepancies remain subject to the existing review guard.
