# Mixed-upload follow-up

Same PAGI-Server repository/branch/PR #23 as read-size-20260925; same immutable runtime snapshots. No local runtime change or push.
Reason: the mixed-download case cannot establish how larger reads affect small GETs competing with uploads. Run this final directional fairness check after the main comparison, never concurrently. One read64 smoke and two rotated four-variant timed rounds, 25 upload clients with 1MiB bodies and 100 small GET clients. Background window 12s, upload window 10s, background starts 0.5s earlier. No further variant or settings introduced.
Deployment: isolated AWS i-077635273be935c19 only; stop after evidence is downloaded.
