package server

import (
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	log "github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/server/router"
)

var (
	logger      = log.GetLogger()
	servicePort string
)

// ServerCmd represents the server command
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServer()
	},
}

func init() {
	ServerCmd.Flags().StringVarP(&servicePort, "port", "p", "8000", "port to run the service on")
}

func runServer() error {
	logger.Info("Starting ai-services server...")
	pclient, err := podman.NewPodmanClient()
	if err != nil {
		return err
	}
	images, err := pclient.ListImages()
	if err != nil {
		return err
	}
	logger.Info("Podman Images", zap.Strings("images", images))
	// Entry point of the server application
	var appRouter = router.CreateRouter()
	logger.Info("ai-services server is up and running", zap.String("port", servicePort))
	logger.Fatal("Error encountered while routing", zap.Error(appRouter.Run(":"+servicePort)))
	return nil
}
