package tasks

import "testing"

func TestValidatePromptTask(t *testing.T) {
	req := Request{Kind: KindPrompt, Task: PromptTask{Prompt: "hello"}}
	if err := ValidateRequest(req); err != nil {
		t.Fatal(err)
	}
	req.Task.Prompt = ""
	if err := ValidateRequest(req); err == nil {
		t.Fatal("expected empty prompt rejection")
	}
}

func TestValidateTemperature(t *testing.T) {
	temp := 3.0
	req := Request{Kind: KindPrompt, Task: PromptTask{Prompt: "hello", Temperature: &temp}}
	if err := ValidateRequest(req); err == nil {
		t.Fatal("expected invalid temperature rejection")
	}
}
