// Code generated -- DO NOT EDIT.
package pb


func NewChannelWithChat(channel *ChatChannel) MsgChannel {
	return MsgChannel{ Type: MsgChannel_Chat, Chat: channel }
}

func NewChannelWithMessage(channel *MessageChannel) MsgChannel {
	return MsgChannel{ Type: MsgChannel_Message, Message: channel }
}

func NewChannelWithFacebook(channel *FacebookChannel) MsgChannel {
	return MsgChannel{ Type: MsgChannel_Facebook, Facebook: channel }
}

func NewChannelWithTwitter(channel *TwitterChannel) MsgChannel {
	return MsgChannel{ Type: MsgChannel_Twitter, Twitter: channel }
}



func NewUserWithChat(user *ChatUser) MsgUser {
	return MsgUser{ Type: MsgUser_Chat, Chat: user }
}

func NewUserWithMessage(user *MessageUser) MsgUser {
	return MsgUser{ Type: MsgUser_Message, Message: user }
}

func NewUserWithFacebook(user *FacebookUser) MsgUser {
	return MsgUser{ Type: MsgUser_Facebook, Facebook: user }
}

func NewUserWithTwitter(user *TwitterUser) MsgUser {
	return MsgUser{ Type: MsgUser_Twitter, Twitter: user }
}
