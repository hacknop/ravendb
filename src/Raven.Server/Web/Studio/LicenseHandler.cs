﻿using System.Threading.Tasks;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class LicenseHandler : RequestHandler
    {
        [RavenAction("/license/status", "GET", AuthorizationStatus.ValidUser)]
        public Task Status()
        {
        
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, ServerStore.LicenseManager.GetLicenseStatus().ToJson());
            }

            return Task.CompletedTask;
        }
 
        [RavenAction("/admin/license/registration", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task Register()
        {
            UserRegistrationInfo userInfo;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(RequestBodyStream(), "license registration form");
                userInfo = JsonDeserializationServer.UserRegistrationInfo(context, json);
            }

            await ServerStore.LicenseManager.RegisterForFreeLicense(userInfo).ConfigureAwait(false);

            NoContentStatus();
        }

        [RavenAction("/admin/license/activate", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task Activate()
        {
            License license;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(RequestBodyStream(), "license activation");
                license = JsonDeserializationServer.License(context, json);
            }

            await ServerStore.LicenseManager.Activate(license, skipLeaseLicense: false);

            NoContentStatus();
        }

        [RavenAction("/admin/license/deactivate", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task Deactivate()
        {
            await ServerStore.LicenseManager.DeactivateLicense();

            NoContentStatus();
        }
    }
}
