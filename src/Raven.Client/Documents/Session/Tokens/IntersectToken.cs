﻿using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class IntersectToken : QueryToken
    {
        private IntersectToken()
        {
        }

        public static IntersectToken Instance = new IntersectToken();

        public override void WriteTo(StringBuilder writer)
        {
            throw new NotImplementedException("Support new syntax: intersect(..., ..., ...)");
            //writer.Append("INTERSECT");
        }
    }
}