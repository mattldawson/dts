// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package transfers

import (
	"github.com/deliveryhero/pipeline/v2"
	"github.com/google/uuid"
)

// sequence of provider-specific stages
type ProviderSequence struct {
	Channels ProviderChannels
	Sequence pipeline.Processor[Task, Task]
}

// channels used to dispatch tasks to provider sequences and return them completed
type ProviderChannels struct {
	Cancel   chan uuid.UUID
	Complete chan Task
	Dispatch chan Task
}

// constructs an appropriate set of channels for a provider given those that communicate with
// pipeline stages
func NewProviderChannels(stageChannels StageChannels) ProviderChannels {
	return ProviderChannels{
		Cancel:   stageChannels.Cancel,
		Complete: stageChannels.Complete,
		Dispatch: make(chan Task, 32), // every provider gets its own dispatch channel
	}
}

func JdpToKBase(channels StageChannels) ProviderSequence {
	return ProviderSequence{
		Channels: NewProviderChannels(channels),
		Sequence: pipeline.Sequence(
			StageFilesAtSource(channels),
			TransferToDestination(channels),
		),
	}
}

func NmdcToKBase(channels StageChannels) ProviderSequence {
	return ProviderSequence{
		Channels: NewProviderChannels(channels),
		Sequence: pipeline.Sequence(
			StageFilesAtSource(channels),
			TransferToDestination(channels),
		),
	}
}
