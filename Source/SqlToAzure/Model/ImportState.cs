﻿using System;

namespace Krowiorsch.Model
{
    public class ImportState
    {
        public ImportState(string identifier, DateTimeOffset maxDate)
        {
            Identifier = identifier;
            Status = KnownStatus.Created;
            LastProcessedTimestamp = MaxDate = maxDate;
        }

        public string Identifier { get; set; }

        public string Status { get; set; }

        public DateTimeOffset MaxDate { get; set; }

        public DateTimeOffset? LastProcessedTimestamp { get; set; }

        public static class KnownStatus
        {
            public const string Created = "Created";
            public const string InProgess = "InProgess";
            public const string Finished = "Finished";
            public const string Error = "Error";

        }
    }
}