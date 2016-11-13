package deployment

func (d *DeployJob) deployAllAtOnce() error {
	log := d.logger.New("fn", "deployAllAtOnce")
	log.Info("starting all-at-once deployment")

	log.Info("scaling up new formation", "release.id", d.NewReleaseID, "processes", d.Processes)
	d.newFormation.Processes = d.Processes
	if err := d.client.Scale(d.newFormation, d.newScaleOptions()); err != nil {
		log.Error("error scaling up new formation", "release.id", d.NewReleaseID, "err", err)
		return err
	}

	log.Info("scaling old formation to zero", "release.id", d.OldReleaseID)
	d.oldFormation.Processes = nil
	if err := d.client.PutFormation(d.oldFormation); err != nil {
		// the new jobs have now started and they are up, so return
		// ErrSkipRollback (rolling back doesn't make a ton of sense
		// because it involves stopping the new working jobs).
		log.Error("error scaling old formation to zero", "release.id", d.OldReleaseID, "err", err)
		return ErrSkipRollback{err.Error()}
	}

	// treat the deployment as finished now (rather than waiting for the
	// jobs to actually stop) as we can trust that the scheduler will
	// actually kill the jobs, so no need to delay the deployment.
	log.Info("finished all-at-once deployment")
	return nil
}
