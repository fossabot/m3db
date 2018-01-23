			logger.Warnf("host doesn't support HugeTLB, proceeding without it")
	hostID, err := cfg.HostID.Resolve()
	if err != nil {
		logger.Fatalf("could not resolve local host ID: %v", err)
	}

		envCfg, err = cfg.EnvironmentConfig.Configure(environment.ConfigurationParameters{
			HostID: hostID,
		})