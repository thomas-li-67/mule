/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.module.launcher;


import static org.mule.config.bootstrap.ArtifactType.APP;
import static org.mule.config.bootstrap.ArtifactType.DOMAIN;
import org.mule.api.MuleContext;
import org.mule.config.bootstrap.ArtifactType;

/**
 * Adapts an {@link ArtifactDeploymentListener} to work as a {@link DeploymentListener}.
 */
public class ArtifactDeploymentListenerAdapter
{

    private DeploymentListener adaptedDomainDeploymentListener;
    private DeploymentListener adaptedApplicationDeploymentListener;
    private ArtifactDeploymentListener artifactDeploymentListener;

    public ArtifactDeploymentListenerAdapter(ArtifactDeploymentListener artifactDeploymentListener)
    {
        this.artifactDeploymentListener = artifactDeploymentListener;
        adapt();
    }

    private void adapt()
    {
        adaptedDomainDeploymentListener = new AdaptedDeploymentListener(artifactDeploymentListener, DOMAIN);
        adaptedApplicationDeploymentListener = new AdaptedDeploymentListener(artifactDeploymentListener, APP);
    }

    public DeploymentListener getDomainDeploymentListener()
    {
        return adaptedDomainDeploymentListener;
    }

    public DeploymentListener getApplicationDeploymentListener()
    {
        return adaptedApplicationDeploymentListener;
    }

    private static class AdaptedDeploymentListener implements DeploymentListener
    {

        private final ArtifactDeploymentListener artifactDeploymentListener;
        private final ArtifactType artifactType;

        public AdaptedDeploymentListener(ArtifactDeploymentListener artifactDeploymentListener, ArtifactType artifactType)
        {
            this.artifactType = artifactType;
            this.artifactDeploymentListener = artifactDeploymentListener;
        }

        @Override
        public void onDeploymentStart(String artifactName)
        {
            artifactDeploymentListener.onDeploymentStart(artifactType, artifactName);
        }

        @Override
        public void onDeploymentSuccess(String artifactName)
        {
            artifactDeploymentListener.onDeploymentSuccess(artifactType, artifactName);
        }

        @Override
        public void onDeploymentFailure(String artifactName, Throwable cause)
        {
            artifactDeploymentListener.onDeploymentFailure(artifactType, artifactName, cause);
        }

        @Override
        public void onUndeploymentStart(String artifactName)
        {
            artifactDeploymentListener.onUndeploymentStart(artifactType, artifactName);
        }

        @Override
        public void onUndeploymentSuccess(String artifactName)
        {
            artifactDeploymentListener.onUndeploymentSuccess(artifactType, artifactName);
        }

        @Override
        public void onUndeploymentFailure(String artifactName, Throwable cause)
        {
            artifactDeploymentListener.onUndeploymentFailure(artifactType, artifactName, cause);
        }

        @Override
        public void onMuleContextCreated(String artifactName, MuleContext context)
        {
            artifactDeploymentListener.onMuleContextCreated(artifactName, context);
        }

        @Override
        public void onMuleContextInitialised(String artifactName, MuleContext context)
        {
            artifactDeploymentListener.onMuleContextInitialised(artifactName, context);
        }

        @Override
        public void onMuleContextConfigured(String artifactName, MuleContext context)
        {
            artifactDeploymentListener.onMuleContextConfigured(artifactName, context);
        }
    }
}
